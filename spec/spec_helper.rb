require "bundler/setup"
require "backup/backblaze"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  # config.disable_monkey_patching!
  # config.raise_errors_for_deprecations!

  config.expect_with :rspec do |c|
    c.syntax = %i[should expect]
  end
end
