require 'digest'

require_relative 'api_importer'
require_relative 'url_token'

module Backup
  module Backblaze
    # calculates sha1 and uploads file
    # Of course, this entire class is an atomic failure, because the underlying file could change at any point.
    #
    # dst can contain / for namespaces
    class UploadFile
      def initialize account:, src:, bucket_id:, dst:, url_token: nil, content_type: nil
        @account = account
        @src = src
        @dst = dst
        @content_type = content_type
        @bucket_id = bucket_id
        @url_token = url_token
      end

      attr_reader :account, :src, :dst, :bucket_id, :content_type

      def url_token
        @url_token or b2_get_upload_url
      end

      def headers
        # headers all have to be strings, otherwise excon & Net::HTTP choke :-|
        {
          'X-Bz-File-Name'                     => (URI.encode dst.encode 'UTF-8'),
          'X-Bz-Content-Sha1'                  => sha1_digest,
          'Content-Length'                     => content_length.to_s,
          'Content-Type'                       => content_type,

          # optional
          'X-Bz-Info-src_last_modified_millis' => last_modified_millis.to_s,
          'X-Bz-Info-b2-content-disposition'   => content_disposition,
        }.merge(TEST_HEADERS).select{|k,v| v}
      end

      def content_type
        @content_type || 'b2/x-auto'
      end

      # No idea what has to be in here
      def content_disposition
      end

      def content_length
        File.size src
      end

      def sha1
        @sha1 = Digest::SHA1.file src
      end

      def sha1_digest
        @sha1_digest = sha1.hexdigest
      end

      def last_modified_millis
        @last_modified_millis ||= begin
          time = File.lstat(src).mtime
          time.tv_sec * 1000 + time.tv_usec / 1000
        end
      end

      extend ApiImporter

      # needed for retry logic
      def b2_authorize_account(retries = 0)
        account.b2_authorize_account retries
      end

      # returns [upload_url, auth_token]
      # Several files can be uploaded to one url.
      # But uploading files in parallel requires one upload url per thread.
      import_endpoint :b2_get_upload_url do |fn|
        headers = {
          'Authorization' => account.authorization_token,
        }.merge(TEST_HEADERS)
        body_wrap = fn[account.api_url, headers, bucket_id]

        # have to set this here for when this gets called by a retry-sequence
        @url_token = UrlToken.new body_wrap.uploadUrl, body_wrap.authorizationToken
      end

      import_endpoint :b2_upload_file do |fn|
        fn[src, headers, url_token]
      end

      def call
        Backup::Logger.info "upload #{src}"

        # not necessary, but makes the flow of control more obvious in the logs
        url_token

        b2_upload_file
      end

      # Seems this doesn't work. Fails with
      #
      # 400 Missing header: Content-Length
      #
      # Probably because chunked encoding doesn't send an initial Content-Length
      private def excon_stream_upload( upload )
        File.open src do |io|
          chunker = lambda do
            # Excon.defaults[:chunk_size] defaults to 1048576, ie 1MB
            # to_s will convert the nil received after everything is read to the final empty chunk
            io.read(Excon.defaults[:chunk_size]).to_s
          end

          Excon.post url, headers: headers, :request_block => chunker, debug_request: true, debug_response: true, instrumentor: Excon::StandardInstrumentor
        end
      end
    end
  end
end
