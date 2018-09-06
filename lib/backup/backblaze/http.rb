module Backup
  module Backblaze
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

      module_function def b2_start_large_file api_url, auth_headers, body
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_start_large_file",
          **auth_headers,
          body: body.to_json,
          expects: 200

        HashWrap.from_json rsp.body
      end

      module_function def b2_get_upload_part_url api_url, auth_headers, file_id
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_get_upload_part_url",
          **auth_headers,
          body: {fileId: file_id}.to_json,
          expects: 200

        # hash = JSON.parse rsp.body
        # hash['code'] = 'emergency error'
        # rsp.body = hash.to_json
        # rsp.status = 503
        # raise (Excon::Errors::ServiceUnavailable.new "yer died", nil, rsp)
        HashWrap.from_json rsp.body
      end

      # NOTE Is there a way to stream this instead of loading multiple 100M chunks
      # into memory? No, backblaze does not allow parts to use chunked encoding.
      module_function def b2_upload_part upload_url, headers, bytes
        # Yes, this is a different pattern to the other Excon.post calls ¯\_(ツ)_/¯
        # Thread.new{sleep 5; exit!}
        rsp = Excon.post \
          upload_url,
          headers: headers,
          body: bytes,
          expects: 200

        # 200 response will be
        # fileId The unique ID for this file.
        # partNumber Which part this is.
        # contentLength The number of bytes stored in the part.
        # contentSha1 The SHA1 of the bytes stored in the part.


        HashWrap.from_json rsp.body
      end

      module_function def b2_finish_large_file api_url, auth_headers, file_id, shas
        rsp = Excon.post \
          "#{api_url}/b2api/v1/b2_finish_large_file",
          **auth_headers,
          body: {fileId: file_id, partSha1Array: shas }.to_json,
          expects: 200

        HashWrap.from_json rsp.body
      end
    end
  end
end
