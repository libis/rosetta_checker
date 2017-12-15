module Libis
  module RosettaChecker
    class FilesToIngestCleanupOptions
      attr_accessor :command, :directory, :recursive, :report, :report_file, :delete

      def initialize(command)
        self.command = command
        self.delete = false
        self.recursive = false
        self.report = true
        default_report_file = "#{command.split(' ').last}-YYYYMMDD-HHMMSS.csv"
        self.report_file = default_report_file
      end

      # @param [OptionParser] parser
      def define(parser)
        parser.banner = "Usage: #{command} [options]"
        parser.separator ''
        parser.separator 'with options:'
        define_directory parser
        define_recursive parser
        define_report parser
        define_report_file parser
        define_delete parser
      end

      # @param [OptionParser] parser
      def define_directory(parser)
        parser.on '-d', '--directory [DIRECTORY]', 'Directory to parse and unclutter (required)' do |dir|
          raise ArgumentError, "Directory '#{dir}' does not exist" unless Dir.exist?(dir)
          raise ArgumentError, "Directory '#{dir}' cannot be read" unless File.readable?(dir)
          self.directory = dir
        end
      end

      # @param [OptionParser] parser
      def define_recursive(parser)
        parser.on '-R', '--[no-]recursive', "Parse through subdirectories [#{self.recursive}]" do |flag|
          self.recursive = flag
        end
      end

      # @param [OptionParser] parser
      def define_report(parser)
        parser.on '-r', '--[no-]report', "Create a report file [#{self.report}]" do |flag|
          self.report = flag
        end
      end

      # @param [OptionParser] parser
      def define_report_file(parser)
        parser.on '-f', '--report-file [FILE]',
                  "File name for the report, if enabled [#{self.report_file}]" do |file|
          self.report_file = file
        end
      end

      # @param [OptionParser] parser
      def define_delete(parser)
        parser.on '-D', '--[no-]delete',
                  "Perform file deletes when file is ingested [#{self.delete}]" do |flag|
          self.delete = flag
        end
      end

    end
  end
end
