require 'optionparser'
require 'digest'
require 'bzip2/ffi'
require 'zip'
require 'oci8'
require 'logging'
require 'pathname'
require 'csv'

require_relative 'sub_command'
require_relative 'options/files_to_ingest_cleanup'

module Libis
  module RosettaChecker
    class FilesToIngestCleanup < SubCommand

      def self.short_desc
        'Report on files that are/are not ingested'.freeze
      end

      def self.command
        'files2ingest'.freeze
      end

      def self.options_class
        FilesToIngestCleanupOptions
      end

      def self.run
        super do |cfg|
          self.new(cfg).run(ARGV)
        end
      end

      attr_accessor :cfg, :logger, :connection, :cursor, :report

      def initialize(cfg)
        @cfg = cfg

        setup_logging

        setup_db
      end

      def finalize
        cursor.close if cursor
        connection.logoff if connection
      end

      def run(argv)
        raise ArgumentError, 'Need to specify at least a directory/file to parse' unless argv.size > 0
        while (dir = argv.shift)
          process_dir dir
          next if argv.empty?
          self.class.parse_options(argv)
          setup_logging
        end
      end

      protected

      SQL_DATA = %w'ie_id rep_id fl_id original_name owner label group_id entity_type user_c'
      CSV_HEADER = %w'parent_type parent file size md5 found name_match' + SQL_DATA

      LOG_PATTERN = "[%d #%p] %-5l : %m\n".freeze

      MSG_CALC_FC = '  - Calculating filesize and checksum'.freeze
      MSG_CHCK_DB = '  - Checking file in DB'.freeze
      MSG_DEFLATE = '  - Deflating'.freeze

      FIND_SQL = <<-SQL
        SELECT
          CONCAT(sp.VALUE, ps.INDEX_LOCATION) as path,
          ps.FILE_SIZE as filesize,
          ps.CHECK_SUM_TYPE as checksum_type,
          ps.CHECK_SUM as checksum,
          sr.FILEORIGINALNAME as original_name,
          ps.STORED_ENTITY_ID as fl_id,
          cr.PID as rep_id,
          ci.PID as ie_id,
          ci.OWNER as owner,
          ci.LABEL as label,
          cf.GROUPID as group_id,
        	ci.ENTITYTYPE as entity_type,
        	ci.PARTITIONC as user_c
        FROM
        	V2KU_PER00.PERMANENT_INDEX ps
        	LEFT JOIN V2KU_SHR00.STORAGE_PARAMETER sp ON sp.STORAGE_ID = ps.STORAGE_ID
        	LEFT JOIN V2KU_REP00.HDESTREAMREF sr ON sr.PID = ps.STORED_ENTITY_ID
        	LEFT JOIN V2KU_REP00.HDECONTROL cf ON cf.PID = ps.STORED_ENTITY_ID
        	LEFT JOIN V2KU_REP00.HDECONTROL cr ON cr.PID = cf.PARENTID
        	LEFT JOIN V2KU_REP00.HDECONTROL ci ON ci.PID = cr.PARENTID
        WHERE
        	sp."KEY" = 'DIR_ROOT'
        AND	ps.FILE_SIZE = :filesize
        AND ps.CHECK_SUM = :checksum
        AND ps.CHECK_SUM_TYPE = 'MD5'
        AND cf.OBJECTTYPE = 'FILE'
        AND cr.OBJECTTYPE = 'REPRESENTATION'
        AND ci.OBJECTTYPE = 'INTELLECTUAL_ENTITY'
      SQL

      def setup_logging
        Logging.logger.root.level = :info
        @logger = Logging.logger[self.class.command]
        @logger.appenders = [Logging.appenders.stdout]
        Logging.appenders.stdout.level = (@cfg.quiet ? :warn : :info)
        @cfg.log_file = nil if @cfg.log_file&.chomp&.strip&.empty?
        @logger.add_appenders Logging.appenders.file(
            @cfg.log_file,
            truncate: false,
            layout: Logging.layouts.pattern(pattern: LOG_PATTERN)
        ) if @cfg.log_file
      end

      def setup_db
        @connection = OCI8.new(@cfg.dbuser, @cfg.dbpass, @cfg.dburl)
        @cursor = @connection.parse(FIND_SQL)
        @cursor.prefetch_rows = 10
      end

      def process_dir(dir)
        if File.directory?(dir)
          unless Dir.exist?(dir)
            logger.error "Directory '#{dir}' does not exist"
            return nil
          end
          unless File.readable?(dir)
            logger.error "Directory '#{dir}' cannot be read"
            return nil
          end
          logger.info "Processing dir '#{dir}'"
          Dir.entries(dir).each do |entry|
            next if %w'. ..'.include? entry
            path = File.join dir, entry
            begin
              process_dir path if @cfg.recursive
              next
            end if File.directory?(path)
            process_file path
          end
        elsif dir[0] == '@'
          file = to_file(dir[1..-1])
          return nil unless file
          logger.info "Processing input file '#{file}'"
          File.open(file, 'r').each_line do |line|
            process_file(line.chomp, File.dirname(file))
          end
        elsif File.file?(dir)
          process_file(dir)
        else
          raise ArgumentError, "Argument '#{dir}' should refer to an existing and readable file or directory"
        end
      end

      def to_file(file, *search_dirs)
        if File.exist?(file)
          return file if File.readable?(file)
          logger.error "File '#{file}' cannot be read"
          return nil
        end
        search_dirs.each do |dir|
          f = File.join(dir, file)
          if File.exist? f
            return f if File.readable?(f)
            logger.error "File '#{f}' cannot be read"
            return nil
          end
        end
        logger.error "File '#{file}' does not exist"
        nil
      end

      def process_file(_file, *search_dir)
        file = to_file(_file, *search_dir)
        return unless file

        if File.directory?(file)
          process_dir(file)
          return
        end

        info = {
            parent_type: 'D',
            parent: File.dirname(file),
            file: File.basename(file)
        }

        logger.info "- #{file}"
        if File.extname(file) == '.bz2'
          logger.info MSG_DEFLATE
          info[:size] = 0
          reader = Bzip2::FFI::Reader.open file
          md5 = Digest::MD5.new
          logger.info MSG_CALC_FC
          while (data = reader.read 2048000) do
            info[:size] += data.length
            md5 << data
          end
          reader.close
          info[:parent_type] = 'F'
          info[:parent] = file
          info[:file] = File.basename file, '.bz2'
          info[:md5] = md5.hexdigest
          check_file info
        elsif File.extname(file) == '.zip'
          logger.info '  - Unpacking'.freeze
          info[:parent_type] = 'Z'
          info[:parent] = file
          Zip::File.open(file) do |zip|
            zip.each do |entry|
              next if entry.directory?
              info[:file] = entry.name
              logger.info "- #{file}/#{entry.name}"
              logger.info MSG_CALC_FC
              info[:size] = 0
              md5 = Digest::MD5.new
              reader = entry.get_input_stream
              while (data = reader.read 2048000) do
                info[:size] += data.length
                md5 << data
              end
              reader.close
              info[:md5] = md5.hexdigest
              check_file info
            end
          end
        else
          begin
            logger.info MSG_CALC_FC
            info[:size] = File.size file
            info[:md5] = Digest::MD5.file file
            check_file info
          rescue Exception
            logger.error "Could not access file '#{file}'"
          end
        end
      end

      def check_file(info)
        logger.info MSG_CHCK_DB

        cursor.bind_param(':filesize', info[:size].to_i)
        cursor.bind_param(':checksum', info[:md5].to_s)

        cursor.exec

        while (found = cursor.fetch_hash)
          SQL_DATA.each {|x| info[x.to_sym] = found[x.upcase]}
          logger.info "    found match: #{info[:ie_id]}/#{info[:rep_id]}/#{info[:fl_id]}"
          if info[:original_name] =~ Regexp.new(info[:file].split(/[ #._-]/).join('.*'))
            logger.info "    name matches: #{info[:original_name]}"
            info[:name_match] = true
          else
            info[:name_match] = false
          end

        end

        info[:found] = cursor.row_count

        to_report(info)

      end

      def to_report(info = nil)
        return unless @cfg.report
        unless @report
          @report ||= CSV.open(@cfg.report_file, 'wb')
          @report << CSV_HEADER
        end
        @report << CSV_HEADER.map {|x| info[x.to_sym]} if info
      end

    end
  end
end
