require 'backup'

module Backup
  module Backblaze
    TEST_HEADERS = {}
    # uncomment for testing
    # TEST_HEADERS = {'X-Bz-Test-Mode' => ['fail_some_uploads', 'expire_some_account_authorization_tokens', 'force_cap_exceeded']}
  end
end

require_relative 'backblaze/version'
require_relative 'backblaze/back_blaze'
