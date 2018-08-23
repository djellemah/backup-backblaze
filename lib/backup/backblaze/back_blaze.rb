require 'excon'
require 'base64'
require 'json'
require 'pathname'

require_relative 'upload_file.rb'
require_relative 'upload_large_file.rb'
require_relative 'account.rb'
require_relative 'retry.rb'

require 'backup/storage/base'

# module naming like this is required by Backup to find the storage
module Backup
  module Storage
    # Different naming to module
    class BackBlaze < Base
      include Backup::Storage::Cycler

      class ConfigurationError < Backup::Error; end

      # Values specified in Model DSL:
      # - API credentials
      # - bucket name
      REQUIRED_ATTRS = %i[account_id app_key bucket]
      attr_accessor *REQUIRED_ATTRS

      # - part size for large files
      attr_accessor :part_size

      def initialize(model, storage_id = nil)
        super
        @path ||= '/'
        check_configuration
      end

      protected

      def check_configuration
        not_specified = REQUIRED_ATTRS.reject{|name| send name}
        if not_specified.any?
          raise ConfigurationError, "#{not_specified.join(", ")} required"
        end

        if part_size && part_size < account.minimum_part_size
          raise ConfigurationError, "part_size must be > #{account.minimum_part_size}"
        end
      end

      def remote_relative_pathname
        @remote_relative_pathname ||= Pathname.new(remote_path).relative_path_from(root)
      end

      def root; @root ||= Pathname.new '/'; end
      def tmp_dir; @tmp_dir ||= Pathname.new Config.tmp_path; end

      def working_part_size
        @working_part_size ||= part_size || account.recommended_part_size
      end

      def transfer!
        bucket_id = account.bucket_id bucket_name: bucket

        package.filenames.each do |filename|
          dst = (remote_relative_pathname + filename).to_s
          src_pathname = tmp_dir + filename

          upload =
          if src_pathname.size > working_part_size * 2.5 || src_pathname.size > 5 * 10**9
            Logger.info "Storing Large '#{dst}'"
            ::Backup::Backblaze::UploadLargeFile.new \
              src: src_pathname,
              dst: dst,
              authorization_token: account.authorization_token,
              url: account.api_url,
              part_size: working_part_size,
              bucket_id: bucket_id
          else
            Logger.info "Storing '#{dst}'"

            # TODO could upload several files in parallel with several of these token_provider
            token_provider = ::Backup::Backblaze::Retry::TokenProvider.new do
              account.upload_url bucket_id: bucket_id
            end

            ::Backup::Backblaze::UploadFile.new \
              src: src_pathname.to_s,
              dst: dst,
              token_provider: token_provider
          end

          hash_wrap = upload.call

          Logger.info "'#{dst}' stored at #{hash_wrap.fileName}"
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{package.time}"

        # workaround for stoopid design in Backup
        package_remote_relative_pathname = Pathname.new(remote_path(package)).relative_path_from(root)

        package.filenames.each do |filename|
          dst = (package_remote_relative_pathname + filename).to_s
          Logger.info "Removing file #{dst}"
          account.delete_file bucket, dst
        end
      end

      protected

      def account
        @account ||= begin
          account_deets = {account_id: account_id}
          Logger.info "Account login for #{account_deets.inspect}"
          ::Backup::Backblaze::Account.new account_id: account_id, app_key: app_key
        end
      end
    end
  end
end
