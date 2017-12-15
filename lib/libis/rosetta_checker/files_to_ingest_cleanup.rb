require 'optionparser'
require 'digest'
require 'bzip2/ffi'

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
          raise ArgumentError, 'Need to specify at least a directory to parse' unless cfg.directory
          @cfg = cfg
          process_dir(cfg.directory)
        end
      end

      def self.process_dir(dir)
        Dir.entries(dir).each do |entry|
          next if %w'. ..'.include? entry
          path = File.join dir, entry
          begin
            self.process_dir path if @cfg.recursive
            next
          end if File.directory?(path)
          self.process_file path
        end
      end

      def self.process_file(file)
        if File.extname(file) == '.bz2'
          reader = Bzip2::FFI::Reader.open(file)
          filesize = 0
          md5 = Digest::MD5.new
          while (data = reader.read(2048000)) do
            filesize += data.length
            md5 << data
          end
          reader.close
          md5_checksum = md5.hexdigest
          check_file file, filesize, md5_checksum
        else
          filesize = File.size(file)
          md5_checksum = Digest::MD5.file file
          check_file file, filesize, md5_checksum
        end
      end

      def self.check_file(file, filesize, md5)
        puts "#{file} #{filesize} #{md5}"
        # TODO:
        # - check if file exists in Rosetta DB
        #   - try to find in permanent storage with filesize and md5
        #   - if not found, try to find in operational storage
        #  - make sure oracle connection is open before starting this.
        #  - if found, report IE, REP and FL id of the file
        #  - if not found, report and check config if file needs to be deleted.
      end
    end
  end
end
