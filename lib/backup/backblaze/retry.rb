module Backup
  module Backblaze
    module Retry
      MAX_RETRIES = 3

      # use the url and token returned by the next_url_token block, until we get a reset
      # indicating that we need a new url and token.
      class TokenProvider
        def initialize &next_url_token
          @next_url_token = next_url_token
          reset
        end

        attr_reader :upload_url, :file_auth_token

        def reset
          @upload_url, @file_auth_token = @next_url_token.call
          self
        end
      end

      class TooManyRetries < RuntimeError; end

      # Try up to retries times to call the upload_blk. Recursive.
      #
      # Various errors (passed through from Excon) coming out of upload_blk will
      # be caught. When an error is caught, :reset method called on
      # token_provider.
      #
      # Return whatever upload_blk returns
      def retry_upload retries, token_provider, &upload_blk
        raise TooManyRetries, "max retries is #{MAX_RETRIES}" unless retries < MAX_RETRIES
        sleep retries ** 2 # exponential backoff for retries > 0

        # Called by all the rescue blocks that want to retry.
        # Mainly so we don't make stoopid errors - like leaving out the +1 for one of the calls :-|
        retry_lambda = lambda do
          retry_upload retries + 1, token_provider.reset, &upload_blk
        end

        begin
          upload_blk.call token_provider, retries
        rescue Excon::Errors::Error => ex
          # The most convenient place to log this
          Backup::Logger.info ex.message
          raise
        end

      # Recoverable errors details sourced from:
      #   https://www.backblaze.com/b2/docs/integration_checklist.html
      #   https://www.backblaze.com/b2/docs/uploading.html

      # socket-related, 408, and 429
      rescue Excon::Errors::SocketError, Excon::Errors::Timeout, Excon::Errors::RequestTimeout, Excon::Errors::TooManyRequests
        retry_lambda.call

      # some 401
      rescue Excon::Errors::Unauthorized => ex
        hw = HashWrap.from_json ex.response.body
        case hw.code
        when 'bad_auth_token', 'expired_auth_token'
          retry_lambda.call
        else
          raise
        end

      # 500-599 where the BackBlaze "code" doesn't matter
      rescue Excon::Errors::HTTPStatusError => ex
        if (500..599) === ex.response.status
          retry_lambda.call
        else
          raise
        end

      end
    end
  end
end
