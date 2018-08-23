require 'base64'
require 'excon'
require 'json'

require_relative 'hash_wrap'

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

      def auth!
        # first call b2_authorize_account to get an account_auth_token
        encoded = Base64.strict_encode64 "#{account_id}:#{app_key}"
        rsp = Excon.get \
          'https://api.backblazeb2.com/b2api/v1/b2_authorize_account',
          headers: {'Authorization' => "Basic #{encoded}"},
          expects: 200

        # this has to stick around because it has various important data
        @body = HashWrap.from_json rsp.body

        unless body.allowed.capabilities.include? 'writeFiles'
          raise "app_key #{app_key} does not have write access to account #{account_id}"
        end
      end

      def auth_headers
        {headers: {'Authorization' => authorization_token}}
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

      # returns [upload_url, auth_token]
      # Several files can be uploaded to one url.
      # But uploading files in parallel requires one upload url per thread.
      def upload_url bucket_id:
        # get the upload url for a specific bucket id. Buckets can be named.
        body = {bucketId: bucket_id }
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_get_upload_url",
          **auth_headers,
          body: body.to_json,
          expects: 200

        hw = HashWrap.from_json rsp.body
        return hw.uploadUrl, hw.authorizationToken
      end

      # return id for given name, or nil if no such named bucket
      def bucket_id bucket_name:
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_buckets",
          **auth_headers,
          body: {bucketName: bucket_name, accountId: account_id}.to_json,
          expects: 200

        buckets = (JSON.parse rsp.body)['buckets']
        found = buckets.find do |ha|
          ha['bucketName'] == bucket_name
        end
        found&.dig 'bucketId' or raise NotFound, "no bucket named #{bucket_name}"
      end

      # Hurhur
      def bucket_list bucket_id: nil
        b2_list_buckets bucketId: bucket_id, accountId: account_id
      end

      def b2_list_buckets body
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_buckets",
          **auth_headers,
          body: body.select{|_,v|v}.to_json,
          expects: 200

        HashWrap.from_json rsp
      end

      # This might be dangerous because large number of file names might come back.
      # But I'm not worrying about that now. Maybe later. Anyway, that's what
      # nextFile and startFile are for.
      def files bucket_name
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_file_names",
          **auth_headers,
          body: {bucketId: (bucket_id bucket_name: bucket_name)}.to_json,
          expects: 200

        # ignoring the top-level {files:, nextFileName:} structure
        files_hash = (JSON.parse rsp.body)['files']

        # ignoring the top-level {files:, nextFileName:} structure
        files_hash.map do |file_info_hash|
          HashWrap.new file_info_hash
        end
      end

      # This is mostly used to get a fileId for a given fileName
      def file_info bucket_name, filename
        # It's too much of a PITA to make this Excon call in only one place
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_list_file_names",
          **auth_headers,
          body: {bucketId: (bucket_id bucket_name: bucket_name), maxFileCount: 1, startFileName: filename}.to_json,
          expects: 200

        files_hash = (JSON.parse rsp.body)['files']

        raise NotFound, "#{filename} not found" unless files_hash.size == 1

        HashWrap.new files_hash.first
      end

      # delete the named file in the named bucket
      # https://www.backblaze.com/b2/docs/b2_delete_file_version.html
      def delete_file bucket_name, filename
        # lookup fileId from given filename
        info = file_info bucket_name, filename

        # delete the fileId
        Excon.post \
          "#{api_url}/b2api/v1/b2_delete_file_version",
          **auth_headers,
          body: {fileName: filename, fileId: info.fileId}.to_json,
          expects: 200

      # ignore 400 with body containing "code": "file_not_present"
      rescue Excon::Errors::BadRequest => ex
        hw = HashWrap.from_json ex.response.body
        raise unless hw.code == 'file_not_present'
      end
    end
  end
end
