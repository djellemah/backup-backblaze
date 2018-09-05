require_relative 'hash_wrap'
require_relative 'api'

module Backup
  module Backblaze
    class Account
      def initialize account_id:, app_key:
        @account_id = account_id
        @app_key = app_key
        auth!
      end

      attr_reader :account_id, :app_key, :body

      class NotFound < RuntimeError; end

      extend Api

      import_endpoint :b2_authorize_account do |fn|
        # @body will be a Hashwrap
        # TODO rename this to body_wrap

        # have to set this here for retry-sequence
        @body = fn[account_id, app_key]
      end

      # make sure all necessary api calls are implemented by this class.
      validate_endpoint_dependencies

      # This can be called by retry paths for various api calls. So it might end
      # up needing synchronisation of some kind.
      def auth!
        # first call b2_authorize_account to get an account_auth_token
        # this has to stick around because it has various important data
        b2_authorize_account

        unless body.allowed.capabilities.include? 'writeFiles'
          raise "app_key #{app_key} does not have write access to account #{account_id}"
        end
      end

      def auth_headers
        Hash headers: {
          'Authorization' => authorization_token,
        }.merge(TEST_HEADERS)
      end

      def api_url
        body.apiUrl or raise NotFound, 'apiUrl'
      end

      def authorization_token
        body.authorizationToken or raise NotFound, 'authorizationToken'
      end

      def minimum_part_size
        # why b2 has this as well as minimumPartSize ¯\_(ツ)_/¯
        body.absoluteMinimumPartSize
      end

      def recommended_part_size
        body.recommendedPartSize
      end

      # The following is leaning towards Bucket.new account, bucket_id/bucket_name
      # body is a hash of string => string
      import_endpoint :b2_list_buckets do |fn, body|
        body_wrap = fn[api_url, auth_headers, body]
      end

      # return id for given name, or nil if no such named bucket
      def bucket_id bucket_name:
        buckets = b2_list_buckets(0, bucketName: bucket_name, accountId: account_id).buckets
        found = buckets.find{|hw| hw.bucketName == bucket_name}
        found&.bucketId or raise NotFound, "no bucket named #{bucket_name}"
      end

      # Hurhur
      def bucket_list bucket_id: nil
        b2_list_buckets 0, bucketId: bucket_id, accountId: account_id
      end

      import_endpoint :b2_list_file_names do |fn, body|
        fn[api_url, auth_headers, body]
      end

      # This might be dangerous because large number of file names might come back.
      # But I'm not worrying about that now. Maybe later. Anyway, that's what
      # nextFile and startFile are for.
      def files bucket_name
        body_wrap = b2_list_file_names 0, bucketId: (bucket_id bucket_name: bucket_name)
        # ignoring the top-level {files:, nextFileName:} structure
        body_wrap.files
      end

      # This is mostly used to get a fileId for a given fileName
      def file_info bucket_name, filename
        body_wrap = b2_list_file_names 0, bucketId: (bucket_id bucket_name: bucket_name), maxFileCount: 1, startFileName: filename
        files_hash = body_wrap.files
        raise NotFound, "#{filename} not found" unless files_hash.size == 1
        files_hash.first
      end

      # delete the named file in the named bucket
      import_endpoint :b2_delete_file_version do |fn, body|
        fn[api_url, auth_headers, body]
      end

      def delete_file bucket_name, filename
        # lookup fileId from given filename
        info = file_info bucket_name, filename
        body_wrap = b2_delete_file_version 0, fileId: info.fileId, fileName: filename

      # ignore 400 with body containing "code": "file_not_present"
      rescue Excon::Errors::BadRequest => ex
        body_wrap = HashWrap.from_json ex.response.body
        raise unless body_wrap.code == 'file_not_present'
      end
    end
  end
end
