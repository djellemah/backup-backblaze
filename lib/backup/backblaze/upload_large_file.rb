require 'digest'

require_relative 'api_importer'
require_relative 'url_token'

module Backup
  module Backblaze
    # Upload a large file in several parts.
    class UploadLargeFile
      # src is a Pathname
      # dst is a String
      def initialize account:, src:, bucket_id:, dst:, url_token: nil, part_size:, content_type: nil
        @account = account
        @src = src
        @dst = dst
        @bucket_id = bucket_id
        @content_type = content_type
        @url_token = url_token
        @part_size = part_size
      end

      attr_reader :src, :dst, :account, :url, :content_type, :part_size, :bucket_id

      # same as account
      def auth_headers
        # only cos the double {{}} is a quite ugly :-p
        Hash headers: {
          'Authorization' => account.authorization_token,
        }.merge(TEST_HEADERS)
      end

      def content_type
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

      extend ApiImporter

      # needed for retry logic
      def b2_authorize_account(retries = 0)
        account.b2_authorize_account retries
      end

      import_endpoint :b2_start_large_file do |fn|
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
        body_wrap = fn[account.api_url, auth_headers, body]
        @file_id = body_wrap.fileId
      end

      def file_id
        @file_id or b2_start_large_file
      end

      def url_token
        @url_token or b2_get_upload_part_url
      end

      import_endpoint :b2_get_upload_part_url do |fn|
        body_wrap = fn[account.api_url, auth_headers, file_id]
        @url_token = UrlToken.new body_wrap.uploadUrl, body_wrap.authorizationToken
      end

      def part_count
        @part_count ||= (src.size / part_size.to_r).ceil
      end

      # NOTE Is there a way to stream this instead of loading multiple 100M chunks
      # into memory? No, backblaze does not allow parts to use chunked encoding.
      import_endpoint :b2_upload_part do |fn, sequence, bytes, sha|
        Backup::Logger.info "#{src} trying part #{sequence + 1} of #{part_count}"

        # not the same as the auth_headers value
        headers = {
          'Authorization' => url_token.auth,
          # cos backblaze wants 1-based, but we want 0-based for reading file
          'X-Bz-Part-Number' => sequence + 1,
          'Content-Length' => bytes.length,
          'X-Bz-Content-Sha1' => sha,
        }.merge(TEST_HEADERS)

        fn[url_token.url, headers, bytes]
      end

      import_endpoint :b2_finish_large_file do |fn, shas|
        fn[account.api_url, auth_headers, file_id, shas]
      end

      # 10000 is backblaze specified max number of parts
      MAX_PARTS = 10000

      def call
        if src.size > part_size * MAX_PARTS
          raise Error, "File #{src.to_s} has size #{src.size} which is larger than part_size * MAX_PARTS #{part_size * MAX_PARTS}. Try increasing part_size in model."
        end

        # try to re-use existing url token if there is one
        url_token

        shas = (0...MAX_PARTS).each_with_object [] do |sequence, shas|
          # read length, offset
          bytes = src.read part_size, part_size * sequence

          if bytes.nil? || bytes.empty?
            # no more file to send
            break shas
          else
            sha = Digest::SHA1.hexdigest bytes
            b2_upload_part 0, sequence, bytes, sha
            Backup::Logger.info "#{src} stored part #{sequence + 1} with #{sha}"
            shas << sha
          end
        end

        # finish up, log and return the response
        hash_wrap = b2_finish_large_file 0, shas
        Backup::Logger.info "#{src} finished"
        hash_wrap
      end
    end
  end
end
