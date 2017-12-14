require "libis/rosetta_checker/version"
require 'optionparser'

module Libis
  module RosettaChecker
    autoload :FilesToIngestChecker, 'lib/libis/rosetta_checker/files_to_ingest_cleanup'

    def run
      subcommands = {
          file2ingest: FilesToIngestCleanup,
          fixity: nil
      }

      command = ARGV.shift
      cmd_class = nil
      cmd_class = subcommands[command.to_sym] unless command.nil?
      if cmd_class.nil?
        if command == 'help'
          command = ARGV.shift
          cmd_class = subcommands[command.to_sym] unless command.nil?
         if cmd_class.nil?
           cmd_class.help
           exit
         end
        end
        puts "Usage: #{ARGV[0]} [command [cmd_options]]"
        puts ''
        puts 'Commands are:'
        puts '  help : Show more help for a specific command'
        subcommands.each do |k,v|
          next unless v
          puts "  #{k.to_s} : #{v.short_desc}"
        end
        puts ''
        puts "See '#{ARGV[0]} help COMMAND' or '#{ARGV[0]} COMMAND --help' for more information."
      end

    end
  end
end
