require 'spec_helper'

def random_hex digits = 12
  domain = (0..9).to_a + (?a..?f).to_a
  domain.sample(digits).join
end

RSpec.describe Backup::Backblaze::Retry do
  MAX_RETRIES = Backup::Backblaze::Retry::MAX_RETRIES

  def bad_auth_401_body
    Hash body: {code: 'bad_auth_token'}.to_json, status: 401
  end

  def expired_auth_401_body
    Hash body: {code: 'expired_auth_token'}.to_json, status: 401
  end

  def expired_auth_401_body
    Hash body: {code: 'expired_auth_token'}.to_json, status: 401
  end

  def server_503_body
    Hash body: {code: 'test service unavailable'}.to_json, status: 503
  end

  def server_403_body
    Hash body: {code: 'ye macht nae pass'}.to_json, status: 403
  end

  def unknown_401_body
    Hash body: {code: 'invalid_account'}.to_json, status: 401
  end

  def server_408_body
    Hash body: {code: 'slow_client'}.to_json, status: 408
  end

  def server_429_body
    Hash body: {code: 'servers_overloaded'}.to_json, status: 429, headers: {'Retry-After' => 20}
  end

  def stub_b2_authorize_account
    Excon.stub \
      (Hash path: '/b2api/v1/b2_authorize_account'),
      (Hash :body => (YAML.load <<-EOY).to_json, :status => 200)
        :absoluteMinimumPartSize: 30000
        :recommendedPartSize: 30000
        :apiUrl: http://test.or/api
        :downloadUrl: download
        :allowed:
          :capabilities:
          - writeFiles
          - listFiles
          - listBuckets
          - deleteFiles
        :accountId: c0ffee
        :authorizationToken: subway
      EOY
  end

  before :all do
    # Backup::Logger.start!
    Excon.defaults[:mock] = true
    # To make stubs thread-local
    # Excon.defaults[:stubs] = :local

    # issues with gem install and executable hooks https://github.com/rvm/executable-hooks/issues/33

    # newer rspec causing pain for old syntax
    RSpec::Mocks::Syntax.instance_variable_set :@warn_about_should, false

    # Don't call Kernel.sleep, we'd rather run the tests fast.
    def (Backup::Backblaze::Retry).sleep *_; end
  end

  before :each do
    Excon.stubs.clear

    # generic catch for REPL-based development ;-)
    Excon.stub \
      Hash.new,
      -> req do
        rsp = {:body => {code: 'all stubbed error'}, :status => 500}
        hw = Backup::Backblaze::HashWrap.new req
        binding.pry
        rsp.to_json
      end

    stub_b2_authorize_account
  end

  let :account do
    Backup::Backblaze::Account.new account_id: 'c0ffee', app_key: '7ea'
  end

  # test for call that backs up to b2_authorize_account
  describe 'b2_list_buckets' do
    def stub_b2_list_buckets( body = nil )
      Excon.stub \
        (Hash path: '/api/b2api/v1/b2_list_buckets'),
        body || (Hash body: (YAML.load <<-EOY).to_json, status: 200)
          buckets:
          - accountId: d54a2f1e963b
            bucketId: 53e4dc20f68719ab
            bucketInfo: {}
            bucketName: magoodyhey
            bucketType: allPrivate
            corsRules: []
            lifecycleRules: []
            revision: 2
          - accountId: d54a2f1e963b
            bucketId: 9750fb6a1c8d432e
            bucketInfo: {}
            bucketName: rootet
            bucketType: allPrivate
            corsRules: []
            lifecycleRules: []
            revision: 2
        EOY
    end

    it 'success with no retries' do
      account.should_receive(:b2_authorize_account).exactly(0).times
      account.should_receive(:b2_list_buckets).exactly(1).times.and_call_original

      stub_b2_list_buckets
      bucket_list = account.bucket_list
      bucket_list.should be_a(Backup::Backblaze::HashWrap)
      bucket_list.buckets.size.should == 2
    end

    %i[bad_auth_401_body expired_auth_401_body].each do |bad_body|
      it "retries on #{bad_body}" do |_example|
        stub_b2_list_buckets send(bad_body)

        # re-auth because bad_auth_401_body
        account.should_receive(:b2_authorize_account).exactly(MAX_RETRIES).times.and_call_original

        # re-auth succeeds, so try original call again, which will keep failing
        account.should_receive(:b2_list_buckets).exactly(MAX_RETRIES).times.and_call_original

        # and finally raise a too many retries error
        ->{account.bucket_list}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
      end
    end

    # These two have different bad bodies. *and* different exceptions. So don't each them.
    it 'no retry for unknown 401 body code' do
      stub_b2_list_buckets unknown_401_body
      account.should_receive(:b2_authorize_account).exactly(0).times.and_call_original
      account.should_receive(:b2_list_buckets).exactly(1).times.and_call_original
      ->{account.bucket_list}.should raise_error(Excon::Errors::Unauthorized)
    end

    it 'no retry for 403' do
      stub_b2_list_buckets server_403_body
      account.should_receive(:b2_authorize_account).exactly(0).times
      account.should_receive(:b2_list_buckets).exactly(1).times.and_call_original
      ->{account.bucket_list}.should raise_error(Excon::Errors::Forbidden)
    end

    %i[server_503_body server_408_body].each do |bad_body|
      it "retries on #{bad_body}, without auth retry" do
        stub_b2_list_buckets send(bad_body)
        account.should_receive(:b2_authorize_account).exactly(0).times
        account.should_receive(:b2_list_buckets).exactly(MAX_RETRIES+1).times.and_call_original
        ->{account.bucket_list}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
      end
    end

    it 'retries for 429, no auth retry, with Retry-After' do
      stub_b2_list_buckets server_429_body
      account.should_receive(:b2_authorize_account).exactly(0).times

      # not sure why we need +1 here
      account.should_receive(:b2_list_buckets).exactly(MAX_RETRIES+1).times do |*args,**kwargs|
        # backoff is an optional arg
        if backoff = kwargs[:backoff]
          backoff.should == server_429_body[:headers]['Retry-After']
        end

        # Ok now jump through flaming hoops of flaming fire to get the original
        # b2_list_buckets method. Because we can't get at and_call_original from here.
        account.class.instance_method(:b2_list_buckets).bind(account).call(*args, **kwargs)
      end

      ->{account.bucket_list}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
    end
  end

  # test for call that backs up to b2_get_upload_url
  describe 'b2_upload_file' do
    def stub_b2_upload_file( body = nil )
      Excon.stub \
        (Hash path: '/api/b2api/v1/b2_upload_file'),
        body || (Hash :body => (YAML.load <<-EOY).to_json, :status => 200)
          accountId: d765e276730e
          action: upload
          bucketId: dd8786b5eef2c7d66743001e
          contentLength: 6144
          contentSha1: 5ba6cf1b3b3a088d73941052f60e78baf05d91fd
          contentType: application/octet-stream
          fileId: 4_zdd8786b5eef2c7d66743001e_f1096f3027e0b1927_d20180725_m115148_c002_v0001095_t0047
          fileInfo:
            src_last_modified_millis: 1532503455580
          fileName: test_file
          uploadTimestamp: 1532519508000
        EOY
    end

    def stub_b2_get_upload_url( body = nil )
      Excon.stub \
        (Hash path: '/api/b2api/v1/b2_get_upload_url'),
        body || (Hash :body => (YAML.load <<-EOY).to_json, :status => 200)
          :uploadUrl: http://test.or/api/b2api/v1/b2_upload_file
          :authorizationToken: f68719ab53e4dc20
        EOY
    end

    let :tmp_file do
      Tempfile.new
    end

    let :upload_file do
      Backup::Backblaze::UploadFile.new \
        src: tmp_file.path,
        dst: 'dir/not_a_dest',
        account: account,
        bucket_id: '212f1bfa'
    end

    it 'success with no retries' do
      stub_b2_upload_file
      stub_b2_get_upload_url
      upload_file.should_receive(:b2_upload_file).exactly(1).times.and_call_original
      upload_file.should_receive(:b2_get_upload_url).exactly(1).times.and_call_original
      upload_file.call
    end

    %i[bad_auth_401_body expired_auth_401_body].each do |bad_body|
      it "b2_get_upload_url with retries with stub_b2_upload_file on #{bad_body}" do
        stub_b2_upload_file send(bad_body)
        stub_b2_get_upload_url

        account.should_receive(:b2_authorize_account).never
        upload_file.should_receive(:b2_upload_file).exactly(MAX_RETRIES).times.and_call_original
        upload_file.should_receive(:b2_get_upload_url).exactly(MAX_RETRIES+1).times.and_call_original

        ->{upload_file.call}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
      end
    end

    it 'no retry for unknown 401 body code' do
      stub_b2_upload_file unknown_401_body
      stub_b2_get_upload_url

      account.should_receive(:b2_authorize_account).exactly(0).times
      upload_file.should_receive(:b2_get_upload_url).exactly(1).times.and_call_original
      upload_file.should_receive(:b2_upload_file).exactly(1).times.and_call_original

      ->{upload_file.call}.should raise_error(Excon::Errors::Unauthorized)
    end

    it 'retries get_url on 50x server error' do
      stub_b2_upload_file server_503_body
      stub_b2_get_upload_url
      account.should_receive(:b2_authorize_account).exactly(0).times
      upload_file.should_receive(:b2_get_upload_url).exactly(MAX_RETRIES+1).times.and_call_original
      upload_file.should_receive(:b2_upload_file).exactly(MAX_RETRIES).times.and_call_original
      ->{upload_file.call}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
    end

    it 'retries auth on 50x server error' do
      stub_b2_upload_file server_503_body
      stub_b2_get_upload_url

      account.should_receive(:b2_authorize_account).exactly(MAX_RETRIES-1).times
      upload_file.should_receive(:b2_get_upload_url).exactly(MAX_RETRIES+1).times.and_call_original
      upload_file.b2_get_upload_url

      # now break both of them
      stub_b2_get_upload_url bad_auth_401_body
      upload_file.should_receive(:b2_upload_file).exactly(1).times.and_call_original
      ->{upload_file.call}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
    end

    it 'retries for 408, without get_url retry' do
      stub_b2_upload_file server_408_body
      stub_b2_get_upload_url
      account.should_receive(:b2_authorize_account).exactly(0).times
      upload_file.should_receive(:b2_get_upload_url).exactly(MAX_RETRIES+1).times.and_call_original
      upload_file.should_receive(:b2_upload_file).exactly(MAX_RETRIES).times.and_call_original
      ->{upload_file.call}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
    end

    it 'retries for 429, no get_url retry, with Retry-After' do
      stub_b2_upload_file server_429_body
      stub_b2_get_upload_url
      account.should_receive(:b2_authorize_account).exactly(0).times
      upload_file.should_receive(:b2_get_upload_url).exactly(1).times.and_call_original

      upload_file.should_receive(:b2_upload_file).exactly(MAX_RETRIES+1).times do |*args,**kwargs|
        # backoff is an optional arg
        if backoff = kwargs[:backoff]
          backoff.should == server_429_body[:headers]['Retry-After']
        end

        # Ok now jump through flaming hoops of flaming fire to get the original
        #:b2_upload_file method. Because we can't get at and_call_original from here.
        upload_file.class.instance_method(:b2_upload_file).bind(upload_file).call(*args, **kwargs)
      end

      ->{upload_file.call}.should raise_error(Backup::Backblaze::Retry::TooManyRetries)
    end

    it 'no retry for 403' do
      stub_b2_upload_file server_403_body
      stub_b2_get_upload_url
      account.should_receive(:b2_authorize_account).exactly(0).times
      upload_file.should_receive(:b2_get_upload_url).exactly(1).times.and_call_original
      upload_file.should_receive(:b2_upload_file).exactly(1).times.and_call_original
      ->{upload_file.call}.should raise_error(Excon::Errors::Forbidden)
    end
  end
end
