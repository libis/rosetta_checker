require 'optionparser'
require 'digest'
require 'bzip2/ffi'
require 'zip'
require 'oci8'
require 'logger'
require 'pathname'
require 'csv'

require_relative 'sub_command'
require_relative 'options/files_to_ingest_cleanup'

module Libis
  module RosettaChecker
    class FilesToIngestCleanup < SubCommand

      def self.short_desc
        'Report on files that are/are not ingested'
      end

      def self.command
        'files2ingest'
      end

      def self.options_class
        FilesToIngestCleanupOptions
      end

      def self.run
        super do |cfg|
          raise ArgumentError, 'Need to specify at least a directory/file to parse' unless ARGV.size > 0
          self.new(cfg).run(ARGV)
        end
      end

      attr_accessor :cfg, :logger, :connection, :cursor, :report

      def initialize(cfg)
        @cfg = cfg
        if @cfg.report
          @report = CSV.open(@cfg.report_file, 'wb')
          @report << %w'type parent name size md5 found ie rep fl orig_name match owner label groupid entity_type userc'
        end
        @logger = Logger.new($stdout)
        @connection = OCI8.new(@cfg.dbuser, @cfg.dbpass, @cfg.dburl)
        find_sql ||= <<-SQL
        SELECT
          CONCAT(sp.VALUE, ps.INDEX_LOCATION) as path,
          ps.FILE_SIZE as filesize,
          ps.CHECK_SUM_TYPE as checksum_type,
          ps.CHECK_SUM as checksum,
          sr.FILEORIGINALNAME as original_name,
          ps.STORED_ENTITY_ID as file_id,
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
        @cursor = @connection.parse(find_sql)
        @cursor.prefetch_rows = 10
        ObjectSpace.define_finalizer(self, self.class.finalize(connection, cursor, report))
      end

      def self.finalize(connection, cursor, report)
        proc {
          cursor.close if cursor
          connection.logoff if connection
          report.close if report
        }
      end

      def run(argv)
        argv.each {|dir| process_dir dir}
      end

      def process_dir(dir)
        if File.directory?(dir)
          logger.error "Directory '#{dir}' does not exist" unless Dir.exist?(dir)
          logger.error "Directory '#{dir}' cannot be read" unless File.readable?(dir)
          puts "Processing dir '#{dir}' :"
          Dir.entries(dir).each do |entry|
            next if %w'. ..'.include? entry
            path = File.join dir, entry
            begin
              process_dir path if @cfg.recursive
              next
            end if File.directory?(path)
            process_file path
          end
          puts "=== End of  dir '#{dir}' ==="
        elsif dir[0] == '@'
          file = to_file(dir[1..-1])
          return unless file
          puts "Processing input file '#{file}' :"
          File.open(file, 'r').each_line do |line|
            process_file(line.chomp, File.dirname(file))
          end
          puts "=== End of input file '#{file}' ==="
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

        process_dir(file) if File.directory?(file)

        puts "- #{File.basename(file)}"
        if File.extname(file) == '.bz2'
          puts "  - Deflating"
          filesize = 0
          reader = Bzip2::FFI::Reader.open(file)
          md5 = Digest::MD5.new
          puts "  - Calculating filesize and checksum"
          while (data = reader.read(2048000)) do
            filesize += data.length
            md5 << data
          end
          reader.close
          md5_checksum = md5.hexdigest
          check_file file, File.basename(file, '.*'), filesize, md5_checksum
        elsif File.extname(file) == '.zip'
          puts "  - Unpacking"
          Zip::File.open(file) do |zip|
            zip.each do |entry|
              next if entry.directory?
              puts "  - #{entry.name}"
              puts "    - Calculating filesize and checksum"
              filesize = 0
              md5 = Digest::MD5.new
              reader = entry.get_input_stream
              while (data = reader.read(2048000)) do
                filesize += data.length
                md5 << data
              end
              reader.close
              md5_checksum = md5.hexdigest
              check_file file, entry.name, filesize, md5_checksum
            end
          end
          puts "  === End of ZIP file '#{file}' ==="
        else
          begin
            puts "  - Calculating filesize and checksum"
            filesize = File.size(file)
            md5_checksum = Digest::MD5.file file
            check_file File.dirname(file), File.basename(file), filesize, md5_checksum
          rescue Exception
            logger.error "Could not access file '#{file}'"
          end
        end
      end

      def check_file(parent, file, filesize, md5)
        puts "   - Checking file in DB"

        data = {
            parent_type: File.directory?(parent) ? 'D' : 'F',
            parent: parent,
            file: file,
            size: filesize,
            md5: md5
        }

        cursor.bind_param(':filesize', filesize.to_i)
        cursor.bind_param(':checksum', md5.to_s)

        cursor.exec

        while (found = cursor.fetch_hash)
          %w'IE_ID REP_ID FILE_ID ORIGINAL_NAME OWNER LABEL GROUP_ID ENTITY_TYPE USER_C'.each do |x|
            data[x.downcase.to_sym] = found[x]
          end
          if found['ORIGINAL_NAME'] =~ Regexp.new(file.split(/[ #._-]/).join('.*'))
            data[:name_match] = true
          else
            data[:name_match] = false
          end

        end

        data[:found] = cursor.row_count

        @report << [
            data[:parent_type],
            data[:parent],
            data[:file],
            data[:size],
            data[:md5],
            data[:found],
            data[:ie_id],
            data[:rep_id],
            data[:file_id],
            data[:original_name],
            data[:name_match],
            data[:owner],
            data[:label],
            data[:group_id],
            data[:entity_type],
            data[:user_c]
        ]
      end

    end
  end
end
