require 'digest'

module Backup
  module Backblaze
    # calculates sha1 and uploads file
    # Of course, this entire class is an atomic failure, because the underlying file could change at any point.
    #
    # dst can contain / for namespaces
    class UploadFile
      # NOTE this is the authorization_token from the call to upload_url get the url, NOT the one from the account
      def initialize src:, dst:, authorization_token:, content_type: nil, url:
        @src = src
        @dst = dst
        @authorization_token = authorization_token
        @content_type = content_type
        @url = url
      end

      attr_reader :src, :dst, :authorization_token, :content_type, :url

      def headers
        # headers all have to be strings, otherwise excon & Net::HTTP choke :-|
        {
          'Authorization'                      => authorization_token,
          'X-Bz-File-Name'                     => (URI.encode dst.encode 'UTF-8'),
          'X-Bz-Content-Sha1'                  => sha1_digest,
          'Content-Length'                     => content_length.to_s,
          'Content-Type'                       => content_type,

          # optional
          'X-Bz-Info-src_last_modified_millis' => last_modified_millis.to_s,
          'X-Bz-Info-b2-content-disposition'   => content_disposition,
          # X-Bz-Info-*
        }.select{|k,v| v}
      end

      # TODO mime lookup
      def content_type
        # 'application/octet-stream'
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
      def call
        # debugs = {debug_request: true, debug_response: true, instrumentor: Excon::StandardInstrumentor}
        # rsp = Excon.post url, headers: headers, body: (File.read src), **debugs
        rsp = Excon.post url, headers: headers, body: (File.read src)
        HashWrap.from_json rsp.body
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
