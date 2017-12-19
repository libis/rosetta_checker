require "libis/rosetta_checker/version"
require 'optionparser'
require 'awesome_print'

require_relative 'rosetta_checker/sub_command'
require_relative 'rosetta_checker/files_to_ingest_cleanup'

module Libis
  module RosettaChecker

    def self.main_command
      @main_command ||= File.basename($0)
    end

    def self.subcommands
      @subcommands ||= SubCommand.subcommands
    end

    def self.help
      puts "Usage: #{main_command} [command [cmd_options]]"
      puts ''
      puts 'Commands are:'
      puts '  help : Show more help for a specific command'
      subcommands.each do |k,v|
        puts "  #{k.to_s} : #{v.short_desc}"
      end
      puts ''
      puts "See '#{main_command} help COMMAND' or '#{main_command} COMMAND --help' for more information."
      puts "A more detailed discussion on the toolkit is available on-line on the LIBIS teamwork" +
               " (https://libis.teamwork.com/#/notebooks/168158)."
      exit
    end

    def self.run

      first_command = command = ARGV.shift
      help if command.nil?
      command = ARGV.shift if command == 'help'
      cmd_class = subcommands[command]
      help if cmd_class.nil?
      cmd_class.help if first_command == 'help'
      cmd_class.run
    end

  end
end
