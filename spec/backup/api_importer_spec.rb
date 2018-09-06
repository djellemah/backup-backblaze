require "spec_helper"

RSpec.describe Backup::Backblaze::ApiImporter do
  describe 'dependency checking' do
    class Warnable
      def self.warns; @warns ||= [] end
      def self.warn *args; warns << args end
    end

    it 'warns on missing dependency method' do
      klass = Class.new Warnable do
        extend Backup::Backblaze::ApiImporter

        import_endpoint :b2_upload_file do |fn|
          # do nothing here
        end
      end

      klass.warns.size.should == 1
      first_warning = klass.warns.first.first
      first_warning.should =~ /b2_get_upload_url/
      first_warning.should =~ /not found/
    end

    it 'warns on dependency method missing argument' do
      klass = Class.new Warnable do
        extend Backup::Backblaze::ApiImporter

        def b2_authorize_account; end

        import_endpoint :b2_get_upload_url do |fn|
          # do nothing here
        end
      end

      klass.warns.size.should == 1
      first_warning = klass.warns.first.first
      first_warning.should =~ /b2_authorize_account/
      first_warning.should =~ /at least one argument/
    end

    it 'no warnings for all is fine' do
      klass = Class.new Warnable do
        extend Backup::Backblaze::ApiImporter

        def b2_authorize_account(retries = 1) end

        import_endpoint :b2_get_upload_url do |fn|
        end

        import_endpoint :b2_upload_file do |fn|
          # do nothing here
        end
      end

      klass.warns.should be_empty
    end
  end
end
