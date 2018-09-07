require 'excon'
require 'base64'

require_relative 'hash_wrap'
require_relative 'retry_lookup'
require_relative 'retry'
require_relative 'http'

module Backup
  module Backblaze
    # This is quite complicated and needs some explanation. API retry rules as
    # defined by Backblaze are not simple. See RetryLookup.retry_sequence for a
    # cross-product of all the rules :-O
    #
    # Some failures require a call to another api endpoint to retry. Some can
    # backup by two or more calls to other api endpoints. So we can't just use,
    # say, Excon's retry facility. Also, backblaze sends back a Retry-After
    # value in some cases, which we ought to respect. Excon's built-in retry
    # can't do that.
    #
    # So to handle that, any class that wants to use the retries must define
    # methods with the same names as the symbols in retry_sequence.
    #
    # import_endpoint is an easifying method to help with that. Parameters will
    # be unchanged between retries, but whatever happens in the body of an
    # import_endpoint declaration will be re-evaluated on each retry.
    #
    # Also note that, the upload_xxx calls do not actually exist - they use urls
    # that are returned by calls to get_upload_xxx. For example, there isn't an
    # actual api endpoint b2_upload_file. We just kinda pretend there is to make
    # the retry_sequence lookup work.
    module ApiImporter
      # define a method on the calling instance that hooks into our
      # call retry logic.
      #
      #  - callable is either a Method, or a symbol for a method in Http
      def import_endpoint callable, &bind_blk
        callable_thing, callable_name = case callable
        when Symbol
          [(Http.method callable), callable]
        when Method
          [callable, callable.name]
        else
          raise "dunno what to do with #{callable.inspect}"
        end

        # Warn about missing endpoint dependencies. Code paths with retry are
        # not very likely to be executed. So a warning that they might not work
        # is useful.
        chunks = caller.chunk_while{|l| l !~ /#{__FILE__}.*#{__method__}/}.to_a
        caller_location = chunks.last.first

        Backup::Backblaze::RetryLookup.retry_dependencies[callable_name].each do |dependency_method|
          begin
            m = instance_method dependency_method
            if m.arity == 0
              warn "#{caller_location} #{self.name}##{dependency_method} required by #{callable} must have at least one argument (retries)"
            end
          rescue NameError
            warn "#{caller_location} #{self.name}##{dependency_method} required by #{callable} but it was not found"
          end
        end

        # Define the api method on the class, mainly so we end with an instance
        # method we can call using the symbols in the retry_sequence.
        # define_method callable_name do |*args, retries: 0, backoff: nil|
        define_method callable_name do |*args, retries: 0, backoff: nil, **kwargs|
          begin
            # initiate retries
            Retry.call retries, backoff, callable_name do
              # Execute bind_blk in the context of self, and pass it the
              # callable_thing along with the args. bind_blk must then call
              # callable_thing with whatever arguments it needs.
              # bind_blk can also deal with the return values from callable_thing
              instance_exec callable_thing, *args, **kwargs, &bind_blk
            end
          rescue Retry::RetrySequence => retry_sequence
            retry_sequence.reduce nil do |_rv, method_name|
              if method_name == callable_name
                # we assume that methods with the same name as the original can
                # receive the same set of arguments as specified in the original
                # call.
                send method_name, *args, retries: retries + 1, backoff: retry_sequence.backoff
              else
                send method_name, retries: retries + 1, backoff: retry_sequence.backoff
              end
            end
          end
        end
      end
    end
  end
end
