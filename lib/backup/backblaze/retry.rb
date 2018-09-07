module Backup
  module Backblaze
    module Retry
      MAX_RETRIES = 5

      class TooManyRetries < RuntimeError; end

      # This is raised when a an api endpoint needs to be retried in a
      # complicate way.
      class RetrySequence < StandardError
        def initialize retry_sequence, backoff
          unless retry_sequence.is_a?(Array) && retry_sequence.all?{|s| Symbol === s}
            raise "provide an array of symbols in #{@retry_sequence.inspect}"
          end

          super retry_sequence.inspect
          @retry_sequence = retry_sequence
          @backoff = backoff
        end

        attr_reader :backoff

        def each &blk
          return enum_for :each unless block_given?
          @retry_sequence.each &blk
        end

        include Enumerable
      end

      module_function def call retries, backoff, api_call_name, &blk
        raise TooManyRetries, "max tries is #{MAX_RETRIES}" unless retries < MAX_RETRIES

        # default exponential backoff for retries > 0
        backoff ||= retries ** 2

        # minor avoidance of unnecessary work in sleep if there's no backoff needed.
        if backoff > 0
          ::Backup::Logger.info "calling #{api_call_name} retry #{retries} after sleep #{backoff}"
          sleep backoff
        else
          ::Backup::Logger.info "calling #{api_call_name}"
        end

        # Finally! Do the call.
        blk.call

      rescue Excon::Errors::HTTPStatusError => ex
        Backup::Logger.info ex.message
        # backoff can end up nil, if Retry-After isn't specified.
        backoff = ex.response.headers['Retry-After']&.to_i
        ::Backup::Logger.info "server specified Retry-After of #{backoff.inspect}"
        raise "Retry-After #{backoff} > 60 is too long" if backoff && backoff > 60

        # need to get code from body
        body_wrap = HashWrap.from_json ex.response.body

        # figure out which retry sequence to use
        recovery_sequence = RetryLookup.retry_sequence api_call_name, ex.response.status, body_wrap.code

        # There's a sequence of retries, and we don't know how to hook the
        # return values and parameters together. So make that someone else's
        # problem.
        #
        # TODO possibly just execute the retry sequence here?
        # That's quite hard cos it will have to have access to the calling self
        if recovery_sequence.any?
          ::Backup::Logger.info "recovery sequence of #{recovery_sequence.inspect}"
          raise RetrySequence.new(recovery_sequence, backoff)
        else
          raise
        end

      rescue Excon::Errors::Error => ex
        Backup::Logger.info ex.message
        # Socket errors etc therefore no http status code and no response body.
        # So just retry with default exponential backoff.
        call retries + 1, nil, api_call_name, &blk
      end
    end
  end
end
