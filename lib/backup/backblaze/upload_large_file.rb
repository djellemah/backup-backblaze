require 'digest'
require_relative 'hash_wrap'

module Backup
  module Backblaze
    # Upload a large file in several parts.
    class UploadLargeFile
      # src is a Pathname
      # dst is a String
      def initialize src:, dst:, authorization_token:, content_type: nil, url:, part_size:, bucket_id:
        @src = src
        @dst = dst
        @authorization_token = authorization_token
        @content_type = content_type
        @url = url
        @part_size = part_size
        @bucket_id = bucket_id
      end

      attr_reader :src, :dst, :authorization_token, :url, :content_type, :part_size, :bucket_id

      # TODO same as account
      def auth_headers
        # only cos the double {{}} is a quite ugly :-p
        Hash headers: {'Authorization' => authorization_token}
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
        src.size
      end

      def last_modified_millis
        @last_modified_millis ||= begin
          time = File.lstat(src).mtime
          time.tv_sec * 1000 + time.tv_usec / 1000
        end
      end

      # https://www.backblaze.com/b2/docs/b2_start_large_file.html
      # definitely need fileInfo back from this. Maybe also uploadTimestamp not sure yet.
      def b2_start_large_file
        # Unlike in UploadFile, it's OK to use symbols here cos to_json converts them to strings
        body = {
          bucketId: bucket_id,
          fileName: dst,
          contentType: content_type,
          fileInfo: {
            src_last_modified_millis: last_modified_millis.to_s,
            'b2-content-disposition': content_disposition
            # this seems to be optional, and is hard to calculate for large file up
            # front. So don't send it.
            # large_file_sha1: sha1_digest,
          }.select{|k,v| v}
        }

        rsp = Excon.post \
          "#{url}/b2api/v1/b2_start_large_file",
          **auth_headers,
          body: body.to_json,
          expects: 200

        HashWrap.from_json rsp.body
      end

      def file_id
        @file_id ||= b2_start_large_file.fileId
      end

      def b2_get_upload_part_url
        rsp = Excon.post \
          "#{url}/b2api/v1/b2_get_upload_part_url",
          **auth_headers,
          body: {fileId: file_id}.to_json,
          expects: 200

        hash = JSON.parse rsp.body
        return hash.values_at 'uploadUrl', 'authorizationToken'
      end

      # NOTE Is there a way to stream this instead of loading multiple 100M chunks
      # into memory? No, backblaze does not allow parts to use chunked encoding.
      def b2_upload_part sequence, upload_url, file_auth_token
        # read length, offset
        bytes = src.read part_size, part_size * sequence

        # return nil if the read comes back as a nil, ie no bytes read
        return if bytes.nil? || bytes.empty?

        headers = {
          # not the same as the auth_headers value
          'Authorization' => file_auth_token,
          # cos backblaze wants 1-based, but we want 0-based for reading file
          'X-Bz-Part-Number' => sequence + 1,
          'Content-Length' => bytes.length,
          'X-Bz-Content-Sha1' => (sha = Digest::SHA1.hexdigest bytes),
        }

        # Yes, this is a different pattern to the other Excon.post calls ¯\_(ツ)_/¯
        rsp = Excon.post upload_url, headers: headers, body: bytes, expects: 200

        # response will be
        # fileId The unique ID for this file.
        # partNumber Which part this is.
        # contentLength The number of bytes stored in the part.
        # contentSha1 The SHA1 of the bytes stored in the part.
        sha
      end

      def b2_finish_large_file shas
        rsp = Excon.post \
          "#{url}/b2api/v1/b2_finish_large_file",
          **auth_headers,
          body: {fileId: file_id, partSha1Array: shas }.to_json,
          expects: 200

        HashWrap.from_json rsp.body
      end

      # 10000 is backblaze specified max number of parts
      MAX_PARTS = 10000

      def call
        if src.size > part_size * MAX_PARTS
          raise Error, "File #{src.to_s} has size #{src.size} which is larger part_size * MAX_PARTS #{part_size * MAX_PARTS}. Try increasing part_size in model."
        end

        # TODO could have multiple threads here, each would need a separate url and token.
        upload_url, file_auth_token = b2_get_upload_part_url

        shas = (0...MAX_PARTS).each_with_object [] do |sequence, shas|
          Backup::Logger.info "#{src} trying part #{sequence}"
          sha = b2_upload_part sequence, upload_url, file_auth_token

          # sha will come back as nil once the file is done.
          if sha
            shas << sha
            Backup::Logger.info "#{src} stored part #{sequence} with #{sha}"
          else
            break shas
          end
        end

        b2_finish_large_file shas
      end
    end
  end
end
