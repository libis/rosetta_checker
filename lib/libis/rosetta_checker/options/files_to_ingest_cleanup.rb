module Libis
  module RosettaChecker
    class FilesToIngestCleanupOptions
      attr_accessor :command, :quiet, :log_file, :recursive, :report, :report_file, :delete, :dburl, :dbuser, :dbpass

      def initialize(command)
        self.command = command
        self.delete = false
        self.quiet = false
        self.log_file = nil
        self.recursive = false
        self.report = true
        default_report_file = "#{command.split(' ').last}-#{DateTime.now.strftime('%Y%m%d-%H%M%S')}.csv"
        self.report_file = default_report_file
        self.dburl = '//libis-db-rosetta:1551/ROSETTAP.kuleuven.be'
        self.dbuser = 'V2KU_REP00'
        self.dbpass = 'V2KU_REP00'
      end

      # @param [OptionParser] parser
      def define(parser)
        parser.banner = "Usage: #{command} [options] [[r_options] [directory|[@]file] ...]"
        parser.separator ''
        parser.separator 'This tool will scan directories for files that are/are not ingested in Rosetta.'
        parser.separator ''
        parser.separator 'If a file name preceded with a \'@\' is given as an argument, the file is expected to be a'
        parser.separator 'text file with directory names - one directory per line.'
        parser.separator ''

        parser.separator 'The tool will compare the file sizes, MD5 checksums and file names with the information in the'
        parser.separator 'Rosetta database to determine if a possible match is found.'
        parser.separator 'with [r_options] (can be repeated in between directory/file inputs):'
        define_quiet parser
        define_logfile parser
        define_recursive parser
        parser.separator ''

        parser.separator 'with [options]:'
        define_report parser
        define_report_file parser
        # define_delete parser
        define_dbparams parser
      end

      # @param [OptionParser] parser
      def define_quiet(parser)
        parser.on '-q', '--[no-]quiet', "Be quiet - no logging output on screen [#{self.quiet}]" do |flag|
          self.quiet = flag
        end
      end

      # @param [OptionParser] parser
      def define_logfile(parser)
        parser.on '-l', '--logfile [FILE]',
                  "Send logging output to file - appends to existing file [#{self.log_file}]",
                  '(re)set to empty string for no logging to file' do |file|
          self.log_file = file
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
        parser.on '--[no-]report', "Create a report file - overwrites existing file [#{self.report}]" do |flag|
          self.report = flag
        end
      end

      # @param [OptionParser] parser
      def define_report_file(parser)
        parser.on '-o', '--output-file [FILE]',
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

      # @param [OptionParser] parser
      def define_dbparams(parser)
        parser.on '--db-url [URL]', "Database connection URL [#{self.dburl}]" do |url|
          self.dburl = url
        end
        parser.on '--db-user [USER]', "Database user name [#{self.dbuser}]" do |user|
          self.dbuser = user
        end
        parser.on '--db-pass [PASSWORD]', "Database password [#{self.dbpass}" do |pass|
          self.dbpass = pass
        end
      end

    end
  end
end
