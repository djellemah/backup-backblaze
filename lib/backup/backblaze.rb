require 'bundler'
Bundler.setup

require 'backup'

module Backup
  module Backblaze
  end
end

require_relative 'backblaze/version'
require_relative 'backblaze/back_blaze'
