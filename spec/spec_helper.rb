require 'active_record'
require 'pg'
require 'logger'

ActiveRecord::Base.establish_connection(
  adapter:  'postgresql',
  host:     ENV.fetch('PGHOST',     'localhost'),
  port:     ENV.fetch('PGPORT',     5432).to_i,
  database: ENV.fetch('PGDATABASE', 'pg_lww_sync_test'),
  username: ENV.fetch('PGUSER',     'postgres'),
  password: ENV.fetch('PGPASSWORD', ''),
  pool:     10
)

ActiveRecord::Base.logger = Logger.new(IO::NULL)

unless defined?(Rails)
  module Rails
    def self.env   = ActiveSupport::StringInquirer.new(ENV.fetch('RAILS_ENV', 'test'))
    def self.root  = Pathname.new(File.expand_path('..', __dir__))
    def self.logger = ActiveRecord::Base.logger
  end
end

require 'active_support/all'

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'pg_lww_sync'

Dir[File.expand_path('support/**/*.rb', __dir__)].each { |f| require f }

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed

  config.before(:each) do
    PgLwwSync.instance_variable_set(:@config, nil)
    PgLwwSync.instance_variable_set(:@on_replication_failure, nil)
  end
end
