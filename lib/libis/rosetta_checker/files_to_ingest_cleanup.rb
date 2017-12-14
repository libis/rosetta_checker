require 'optionparser'

module Libis
  module RosettaChecker
    class FilesToIngestCleanup

      def self.short_desc
        'Report on files that are/are not ingested'
      end

      # @param [OptionsParser] parser
      def self.options(parser)
        Options.new.define(parser)
      end

      class Options
        attr_accessor :directory, :report, :report_file, :delete

        def initialize
          self.delete = false
        end

        # @param [OptionParser] parser
        def define(parser)
          parser.banner = 'Usage: Unclutter [options]'
          parser.separator ''
          parser.separator 'with options:'
          define_directory parser
        end

        # @param [OptionParser] parser
        def define_directory(parser)
          parser.on '-d', '--directory [DIRECTORY]', 'Directory to parse and unclutter' do |dir|
            raise ArgumentError, "Directory '#{dir}' does not exist" unless Dir.exist?(dir)
            raise ArgumentError, "Directory '#{dir}' cannot be read" unless File.readable?(dir)
            self.directory = dir
          end
        end

        # @param [OptionParser] parser
        def define_report(parser)
          parser.on '-r', '--[no-]report', 'Create a report file' do |flag|
            self.report = flag
          end
        end

        # @param [OptionParser] parser
        def define_report_file(parser)
          parser.on '-f', '--report-file [FILE]', 'File name for the report, if enabled',
                    'Default file name is unclutter-<timestamp>.csv' do |file|
            self.report_file = file
          end
        end

        # @param [OptionParser] parser
        def define_delete(parser)
          parser.on '-x', '--execute-delete', 'Perform file deletes when ingest check is true' do |flag|
            self.delete = flag
          end
        end
      end

    end
  end
end
