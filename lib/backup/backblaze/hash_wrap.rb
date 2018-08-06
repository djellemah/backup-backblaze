module Backup
  module Backblaze
    # Intended as a quick-n-dirty way to deep-wrap json objects.
    # If it doesn't work for you, rather than scope-creeping this consider: Hash, OpenStruct, a class, etc.
    class HashWrap
      def initialize( hash )
        @hash = hash
      end

      def method_missing(meth, *args, &blk)
        value = @hash.fetch meth.to_s do |_key|
          @hash.fetch meth do |_key|
            super
          end
        end
        __wrap value
      end

      private def __wrap value
        case value
        when Hash
          self.class.new value
        when Array
          value.map do |item|
            __wrap item
          end
        else
          value
        end
      end

      def to_h
        # no, you can't have a copy of this hash to mess with
        @hash.dup
      end

      # really a convenience method
      def self.from_json json
        new JSON.parse json
      end
    end
  end
end
