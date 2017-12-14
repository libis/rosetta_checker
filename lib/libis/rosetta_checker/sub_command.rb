require 'optionparser'

module Libis
  module RosettaChecker
    module SubCommand
      # @param [OptiomParser] parser
      def define_parser(parser)
        self.options.define(parser)
      end
    end
  end
end
