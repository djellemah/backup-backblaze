# Backup::Backblaze

Plugin for the [Backup](https://github.com/backup/backup) gem to use [Backblaze](https://www.backblaze.com/) as storage.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'backup-backblaze'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install backup-backblaze

## Usage

  Add a storage block something like this to your usual Backup DSL file:

    # BackBlaze must be a string here, not a class name. Because it's defined external to Backup gem.
    store_with 'BackBlaze' do |server|
      # from backblaze ui
      server.account_id = 'deadbeefdead'
      server.app_key    = 'c27111357f682232c9943f6e63e98f916722c975e4'

      # bucket name must be globally unique (yes, really).
      # create buckets on the backblaze website. app_key must have access.
      server.bucket     = 'your_globally_unique_bucket_name'

      # path defaults to '/'
      server.path       = '/whatever/you_like'
      server.keep       = 3

      # minimum is 5mb, default is 100mb. Leave at default unless you have a good reason.
      # server.part_size = 5000000
    end


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/djellemah/backup-backblaze.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
