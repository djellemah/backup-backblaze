require "spec_helper"

RSpec.describe Backup::Backblaze do
  it "has a version number" do
    expect(Backup::Backblaze::VERSION).not_to be nil
  end
end
