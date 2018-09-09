require 'set'

module Backup
  module Backblaze
    module RetryLookup
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

      # Generated from retry.pl
      #
      # Cross-product of all the retry scenarios we know about. This probably
      # isn't the fastest way to calculate retries, but retries are rare. So the
      # slowdown doesn't matter.
      module_function def retry_sequence api_call, http_status, code
        case [api_call.to_sym,        http_status, code.to_sym]
        when [:b2_upload_part, 401, :expired_auth_token] then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 401, :bad_auth_token] then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 408, Any] then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 500..599, Any] then [:b2_get_upload_part_url,:b2_upload_part]
        when [:b2_upload_part, 429, Any] then [:b2_upload_part]
        when [:b2_get_upload_part_url, 401, :expired_auth_token] then [:b2_authorize_account,:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 401, :bad_auth_token] then [:b2_authorize_account,:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 408, Any] then [:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 429, Any] then [:b2_get_upload_part_url]
        when [:b2_get_upload_part_url, 500..599, Any] then [:b2_get_upload_part_url]
        when [:b2_get_upload_url, 401, :expired_auth_token] then [:b2_authorize_account,:b2_get_upload_url]
        when [:b2_get_upload_url, 401, :bad_auth_token] then [:b2_authorize_account,:b2_get_upload_url]
        when [:b2_get_upload_url, 408, Any] then [:b2_get_upload_url]
        when [:b2_get_upload_url, 429, Any] then [:b2_get_upload_url]
        when [:b2_get_upload_url, 500..599, Any] then [:b2_get_upload_url]
        when [:b2_upload_file, 401, :expired_auth_token] then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 401, :bad_auth_token] then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 408, Any] then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 500..599, Any] then [:b2_get_upload_url,:b2_upload_file]
        when [:b2_upload_file, 429, Any] then [:b2_upload_file]
        when [:b2_authorize_account, 408, Any] then [:b2_authorize_account]
        when [:b2_authorize_account, 429, Any] then [:b2_authorize_account]
        when [:b2_authorize_account, 500..599, Any] then [:b2_authorize_account]
        when [:b2_list_buckets, 401, :expired_auth_token] then [:b2_authorize_account,:b2_list_buckets]
        when [:b2_list_buckets, 401, :bad_auth_token] then [:b2_authorize_account,:b2_list_buckets]
        when [:b2_list_buckets, 408, Any] then [:b2_list_buckets]
        when [:b2_list_buckets, 429, Any] then [:b2_list_buckets]
        when [:b2_list_buckets, 500..599, Any] then [:b2_list_buckets]
        when [:b2_list_file_names, 401, :expired_auth_token] then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 401, :bad_auth_token] then [:b2_authorize_account,:b2_list_file_names]
        when [:b2_list_file_names, 408, Any] then [:b2_list_file_names]
        when [:b2_list_file_names, 429, Any] then [:b2_list_file_names]
        when [:b2_list_file_names, 500..599, Any] then [:b2_list_file_names]
        when [:b2_delete_file_version, 401, :expired_auth_token] then [:b2_authorize_account,:b2_delete_file_version]
        when [:b2_delete_file_version, 401, :bad_auth_token] then [:b2_authorize_account,:b2_delete_file_version]
        when [:b2_delete_file_version, 408, Any] then [:b2_delete_file_version]
        when [:b2_delete_file_version, 429, Any] then [:b2_delete_file_version]
        when [:b2_delete_file_version, 500..599, Any] then [:b2_delete_file_version]
        when [:b2_finish_large_file, 401, :expired_auth_token] then [:b2_authorize_account,:b2_finish_large_file]
        when [:b2_finish_large_file, 401, :bad_auth_token] then [:b2_authorize_account,:b2_finish_large_file]
        when [:b2_finish_large_file, 408, Any] then [:b2_finish_large_file]
        when [:b2_finish_large_file, 429, Any] then [:b2_finish_large_file]
        when [:b2_finish_large_file, 500..599, Any] then [:b2_finish_large_file]
        when [:b2_start_large_file, 401, :expired_auth_token] then [:b2_authorize_account,:b2_start_large_file]
        when [:b2_start_large_file, 401, :bad_auth_token] then [:b2_authorize_account,:b2_start_large_file]
        when [:b2_start_large_file, 408, Any] then [:b2_start_large_file]
        when [:b2_start_large_file, 429, Any] then [:b2_start_large_file]
        when [:b2_start_large_file, 500..599, Any] then [:b2_start_large_file]
        else [] # No retry. eg 400 and most 401 should just fail immediately
        end
      end

      module_function def retry_dependencies
        @retry_dependencies ||= begin
          # didn't want to fight with prolog to generate uniq values here, so just let ruby do it.
          retries = Hash.new{|h,k| h[k] = Set.new}
          retries[:b2_upload_part].merge([:b2_get_upload_part_url])
          retries[:b2_upload_part].merge([:b2_get_upload_part_url])
          retries[:b2_upload_part].merge([:b2_get_upload_part_url])
          retries[:b2_upload_part].merge([:b2_get_upload_part_url])
          retries[:b2_get_upload_part_url].merge([:b2_authorize_account])
          retries[:b2_get_upload_part_url].merge([:b2_authorize_account])
          retries[:b2_get_upload_url].merge([:b2_authorize_account])
          retries[:b2_get_upload_url].merge([:b2_authorize_account])
          retries[:b2_upload_file].merge([:b2_get_upload_url])
          retries[:b2_upload_file].merge([:b2_get_upload_url])
          retries[:b2_upload_file].merge([:b2_get_upload_url])
          retries[:b2_upload_file].merge([:b2_get_upload_url])
          retries[:b2_list_buckets].merge([:b2_authorize_account])
          retries[:b2_list_buckets].merge([:b2_authorize_account])
          retries[:b2_list_file_names].merge([:b2_authorize_account])
          retries[:b2_list_file_names].merge([:b2_authorize_account])
          retries[:b2_delete_file_version].merge([:b2_authorize_account])
          retries[:b2_delete_file_version].merge([:b2_authorize_account])
          retries[:b2_finish_large_file].merge([:b2_authorize_account])
          retries[:b2_finish_large_file].merge([:b2_authorize_account])
          retries[:b2_start_large_file].merge([:b2_authorize_account])
          retries[:b2_start_large_file].merge([:b2_authorize_account])
          retries
        end
      end
    end
  end
end
