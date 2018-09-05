module Backup
  module Backblaze
    class UrlToken
      def initialize url, auth
        @url, @auth = url, auth
      end

      attr_reader :url, :auth
    end
  end
end
