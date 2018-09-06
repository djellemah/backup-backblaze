require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

task :default => [:spec, :generate_retry]

task :spec => :generate_retry
task :build => :generate_retry

desc 'Generate the retry_lookup.rb file from prolog source'
task :generate_retry => FileList['lib/backup/backblaze/retry_lookup.rb']

SWIPL = 'swipl'
SWIPL_VERSION = '7.4.2'

def chk_swipl_version
  version = `#{SWIPL} --version`
  version =~ /SWI-Prolog version (7.\d+.\d+)/
  raise unless $1 >= SWIPL_VERSION
rescue Errno::ENOENT, RuntimeError
  puts "#{SWIPL} >= #{SWIPL_VERSION} not found on PATH. Install SWI-Prolog version >= #{SWIPL_VERSION} from http://www.swi-prolog.org/Download.html"
  exit 1
end

file 'lib/backup/backblaze/retry_lookup.rb' => %w[src/retry_lookup.erb src/retry.pl] do |task|
  puts "building #{task} from #{task.source} prolog"
  chk_swipl_version
  sh "erb -T- #{task.source} >#{task}"
end
