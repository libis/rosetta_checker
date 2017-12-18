require 'optionparser'

module Libis
  module RosettaChecker
    class SubCommand
      def self.short_desc
        raise RuntimeError, 'Method should be overwritten'
      end

      def self.command
        raise RuntimeError, 'Method should be overwritten'
      end

      def self.options_class
        raise RuntimeError, 'Method should be overwritten'
      end

      def self.help
        self.options('-h')
      end

      def self.options(*argv)
        argv = ARGV if argv.empty?
        @config ||= self.options_class.new "#{RosettaChecker.main_command} #{self.command}"
        OptionParser.new do |opts|
          @config.define opts
          opts.on '-h', '--help', 'Show this help' do
            puts opts
            exit
          end
        end.order!(argv)
      end

      def self.run
        self.options
        yield @config
      rescue OptionParser::ParseError => e
        puts "ERROR: #{e.message}"
        puts ''
        self.help
      rescue StandardError => e
        $stderr.puts "ERROR: #{e.message} @ #{e.backtrace.first}"
          ap e.backtrace
      rescue Interrupt
        $stderr.puts "ERROR: Interrupted."
      end

      def self.subcommands
        Hash[ObjectSpace.each_object(Class).select {|klass| klass < self}.map {|klass| [klass.command, klass]}]
      end
    end
  end
end
