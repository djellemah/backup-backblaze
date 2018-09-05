require 'excon'
require 'base64'

require_relative 'hash_wrap'

module Backup
  module Backblaze
    # This is quite complicated and needs some explanation. API retry rules as
    # defined by Backblaze are not simple. See retry_sequence below for a cross-
    # product of all the rules :-O
    #
    # Some failures require a call to another api endpoint to retry. Some can
    # backup by two or more calls to other api endpoints. So we can't just use,
    # say, Excon's retry facility. Also, backblaze sends back a Retry-After
    # value in some cases, which we ought to respect. Excon's built-in retry
    # can't do that.
    #
    # So to handle that, any class that wants to use the retries must define
    # methods with the same names as the symbols in retry_sequence.
    #
    # import_endpoint is an easifying method to help with that. Parameters will
    # be unchanged between retries, but whatever happens in the body of an
    # import_endpoint declaration will be re-evaluated on each retry.
    #
    # Also note that, the upload_xxx calls do not actually exist - they use urls
    # that are returned by calls to get_upload_xxx. For example, there isn't an
    # actual api endpoint b2_upload_file. We just kinda pretend there is to make
    # the retry_sequence lookup work.
    module Api
      # define a method on the calling instance that hooks into our
      # call retry logic.
      #
      #  - callable is either a Method, or a symbol for a method in Http
      def import_endpoint callable, &bind_blk
        callable_thing, callable_name = case callable
        when Symbol
          [(Http.method callable), callable]
        when Method
          [callable, callable.name]
        else
          raise "dunno what to do with #{callable.inspect}"
        end

        # Define the api method on the class, mainly so we end with an instance
        # method we can call using the symbols in the retry_sequence.
        define_method callable_name do |retries = 0, *args|
          begin
            # initiate retries
            Backoff.call retries, nil, callable_name do
              # Execute bind_blk in the context of self, and pass it the
              # callable_thing along with the args. bind_blk must then call
              # callable_thing with whatever arguments it needs.
              # bind_blk can also deal with the return values from callable_thing
              instance_exec callable_thing, *args, &bind_blk
            end
          rescue Backoff::RetrySequence => retry_sequence
            #  we want the last return value from the retry sequence
            retry_sequence.reduce nil do |_rv, method_name|
              if method_name = callable_name
                # we assume that methods with the same name as the original can
                # receive the same set of arguments as specified in the original
                # call.
                send method_name, retries + 1, *args
              else
                send method_name, retries + 1
              end
            end
          end
        end
      end

      def validate_endpoint_dependencies
        puts "TODO: make sure all imported http calls have their retry dependencies"
      end
    end

    module Backoff
      MAX_RETRIES = 5

      class TooManyRetries < RuntimeError; end

      # This is raised when a an api endpoint needs to be retried in a
      # complicate way.
      class RetrySequence < StandardError
        def initialize retry_sequence
          unless retry_sequence.is_a?(Array) && retry_sequence.all?{|s| Symbol === s}
            raise "provide an array of symbols in #{@retry_sequence.inspect}"
          end

          super retry_sequence.inspect
          @retry_sequence = retry_sequence
        end

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
        recovery_sequence = Backoff.retry_sequence api_call_name, ex.response.status, body_wrap.code

        if recovery_sequence.size == 1 && recovery_sequence.first == api_call_name
          # Retry strategy in this case is to just make the same call again.
          call retries + 1, backoff, api_call_name, &blk
        else
          # There's a sequence of retries, and we don't know how to hook the
          # return values and parameters together. So make that someone else's
          # problem.
          #
          # TODO possibly just execute the retry sequence here?
          # That's quite hard cos it will have to have access to the calling self
          if recovery_sequence.any?
            ::Backup::Logger.info "initiating recovery_sequence of #{recovery_sequence.inspect}"
            raise RetrySequence, recovery_sequence
          else
            raise
          end
        end

      rescue Excon::Errors::Error => ex
        Backup::Logger.info ex.message
        # Socket errors etc therefore no http status code and no response body.
        # So just retry with default exponential backoff.
        call retries + 1, nil, api_call_name, &blk
      end

      def (Any = Object.new).=== other
        true
      end

      module Matcher
        refine Array do
          def === other
            return false unless size == other.size
            size.times.all? do |idx|
              self[idx] === other[idx]
            end
          end
        end
      end

      using Matcher

      # Generated using prolog
      #
      # Cross-product of all the retry scenarios we know about. This probably
      # isn't the fastest way to calculate retries, but retries are rare. So the
      # slowdown doesn't matter.
      module_function def retry_sequence api_call, http_status, code
        case [api_call.to_sym,        http_status, code.to_sym]
        when [:b2_authorize_account, 408, Any]                   then [:b2_authorize_account]
        when [:b2_authorize_account, 429, Any]                   then [:b2_authorize_account]
        when [:b2_authorize_account, 500..599, Any]              then [:b2_authorize_account]
        when [:b2_delete_file_version, 401, :expired_auth_token] then [:b2_authorize_account,:b2_delete_file_version]
        when [:b2_delete_file_version, 401, :bad_auth_token]     then [:b2_authorize_account,:b2_delete_file_version]
        when [:b2_delete_file_version, 408, Any]                 then [:b2_delete_file_version]
        when [:b2_delete_file_version, 429, Any]                 then [:b2_delete_file_version]
        when [:b2_delete_file_version, 500..599, Any]            then [:b2_delete_file_version]
        when [:b2_finish_large_file, 401, :expired_auth_token]   then [:b2_authorize_account,:b2_finish_large_file]
        when [:b2_finish_large_file, 401, :bad_auth_token]       then [:b2_authorize_account,:b2_finish_large_file]
        when [:b2_finish_large_file, 408, Any]                   then [:b2_finish_large_file]
        when [:b2_finish_large_file, 429, Any]                   then [:b2_finish_large_file]
        when [:b2_finish_large_file, 500..599, Any]              then [:b2_finish_large_file]
        when [:b2_get_upload_part_url, 401, :expired_auth_token] then [:b2_authorize_account,:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 401, :bad_auth_token]     then [:b2_authorize_account,:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 408, Any]                 then [:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 429, Any]                 then [:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 500..599, Any]            then [:b2_get_upload_part_url]
        when [:b2_get_upload_url, 401, :expired_auth_token]      then [:b2_authorize_account,:b2_get_upload_url]
        when [:b2_get_upload_url, 401, :bad_auth_token]          then [:b2_authorize_account,:b2_get_upload_url]
        when [:b2_get_upload_url, 408, Any]                      then [:b2_get_upload_url]
        when [:b2_get_upload_url, 429, Any]                      then [:b2_get_upload_url]
        when [:b2_get_upload_url, 500..599, Any]                 then [:b2_get_upload_url]
        when [:b2_list_buckets, 401, :expired_auth_token]        then [:b2_authorize_account,:b2_list_buckets]
        when [:b2_list_buckets, 401, :bad_auth_token]            then [:b2_authorize_account,:b2_list_buckets]
        when [:b2_list_buckets, 408, Any]                        then [:b2_list_buckets]
        when [:b2_list_buckets, 429, Any]                        then [:b2_list_buckets]
        when [:b2_list_buckets, 500..599, Any]                   then [:b2_list_buckets]
        when [:b2_list_file_names, 401, :expired_auth_token]     then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 401, :bad_auth_token]         then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 408, Any]                     then [:b2_list_file_names]
        when [:b2_list_file_names, 429, Any]                     then [:b2_list_file_names]
        when [:b2_list_file_names, 500..599, Any]                then [:b2_list_file_names]
        when [:b2_start_large_file, 401, :expired_auth_token]    then [:b2_authorize_account,:b2_start_large_file]
        when [:b2_start_large_file, 401, :bad_auth_token]        then [:b2_authorize_account,:b2_start_large_file]
        when [:b2_start_large_file, 408, Any]                    then [:b2_start_large_file]
        when [:b2_start_large_file, 429, Any]                    then [:b2_start_large_file]
        when [:b2_start_large_file, 500..599, Any]               then [:b2_start_large_file]
        when [:b2_upload_part, 401, :expired_auth_token]         then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 401, :bad_auth_token]             then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 408, Any]                         then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 500..599, Any]                    then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 429, Any]                         then [:b2_upload_part]
        when [:b2_upload_file, 401, :expired_auth_token]         then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 401, :bad_auth_token]             then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 408, Any]                         then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 500..599, Any]                    then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 429, Any]                         then [:b2_upload_file]
        when [:b2_list_file_names, 401, :expired_auth_token]     then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 401, :bad_auth_token]         then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 408, Any]                     then [:b2_list_file_names]
        when [:b2_list_file_names, 429, Any]                     then [:b2_list_file_names]
        when [:b2_list_file_names, 500..599, Any]                then [:b2_list_file_names]
        else [] # No retry. eg 400 and most 401 should just fail immediately
        end
      end
    end

    module Http
      module_function def b2_authorize_account account_id, app_key
        encoded = Base64.strict_encode64 "#{account_id}:#{app_key}"
        rsp = Excon.get \
          'https://api.backblazeb2.com/b2api/v1/b2_authorize_account',
          headers: {'Authorization' => "Basic #{encoded}"},
          expects: 200
        HashWrap.from_json rsp.body
      end

      module_function def b2_get_upload_url api_url, auth_headers, bucket_id
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_get_upload_url",
          headers: auth_headers,
          body: {bucketId: bucket_id}.to_json,
          expects: 200
        HashWrap.from_json rsp.body
      end

      # upload with incorrect sha1 responds with
      #
      # {"code"=>"bad_request", "message"=>"Sha1 did not match data received", "status"=>400}
      #
      # Normal response
      #
      #{"accountId"=>"d765e276730e",
      # "action"=>"upload",
      # "bucketId"=>"dd8786b5eef2c7d66743001e",
      # "contentLength"=>6144,
      # "contentSha1"=>"5ba6cf1b3b3a088d73941052f60e78baf05d91fd",
      # "contentType"=>"application/octet-stream",
      # "fileId"=>"4_zdd8786b5eef2c7d66743001e_f1096f3027e0b1927_d20180725_m115148_c002_v0001095_t0047",
      # "fileInfo"=>{"src_last_modified_millis"=>"1532503455580"},
      # "fileName"=>"test_file",
      # "uploadTimestamp"=>1532519508000}
      module_function def b2_upload_file src, headers, url_token
        rsp = Excon.post \
          url_token.url,
          headers: (headers.merge 'Authorization' => url_token.auth),
          body: (File.read src),
          expects: 200
        HashWrap.from_json rsp.body
      end

      module_function def b2_list_buckets api_url, auth_headers, body
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_buckets",
          **auth_headers,
          body: body.to_json,
          expects: 200
        HashWrap.from_json rsp.body
      end

      module_function def b2_list_file_names api_url, auth_headers, body
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_file_names",
          **auth_headers,
          body: body.to_json,
          expects: 200
        HashWrap.from_json rsp.body
      end

      # delete the fileId
      module_function def b2_delete_file_version api_url, auth_headers, body
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_delete_file_version",
          **auth_headers,
          body: body.to_json,
          expects: 200
        HashWrap.from_json rsp.body
      end
    end
  end
end
