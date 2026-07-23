require 'spec_helper'
require 'tmpdir'

RSpec.describe PgLwwSync do
  describe '.config' do
    context 'when config file exists' do
      around do |example|
        Dir.mktmpdir do |dir|
          allow(Rails).to receive(:root).and_return(Pathname.new(dir))
          allow(Rails).to receive(:env).and_return(ActiveSupport::StringInquirer.new('test'))
          FileUtils.mkdir_p("#{dir}/config")
          File.write("#{dir}/config/pg_lww_sync.yml", <<~YAML)
            test:
              node_id: "file_node"
              remote_nodes:
                - node_id: "peer_a"
                  host: "peer.internal"
                  database: "app_db"
                  username: "sync"
                  password: "secret"
          YAML
          example.run
        end
      end

      it 'loads node_id from the correct environment section' do
        expect(PgLwwSync.node_id).to eq('file_node')
      end

      it 'loads remote_nodes with symbolized keys' do
        nodes = PgLwwSync.remote_nodes
        expect(nodes.size).to eq(1)
        expect(nodes.first[:node_id]).to eq('peer_a')
        expect(nodes.first[:host]).to eq('peer.internal')
      end
    end

    context 'when config file is missing' do
      around do |example|
        Dir.mktmpdir do |dir|
          allow(Rails).to receive(:root).and_return(Pathname.new(dir))
          example.run
        end
      end

      it 'raises a descriptive error mentioning the generator command' do
        expect { PgLwwSync.config }.to raise_error(RuntimeError, /rails generate pg_lww_sync:install/)
      end
    end

    context 'when config file contains ERB' do
      around do |example|
        Dir.mktmpdir do |dir|
          allow(Rails).to receive(:root).and_return(Pathname.new(dir))
          allow(Rails).to receive(:env).and_return(ActiveSupport::StringInquirer.new('test'))
          FileUtils.mkdir_p("#{dir}/config")
          File.write("#{dir}/config/pg_lww_sync.yml", <<~YAML)
            test:
              node_id: "<%= 'erb_' + 'node' %>"
              remote_nodes: []
          YAML
          example.run
        end
      end

      it 'evaluates ERB before parsing YAML' do
        expect(PgLwwSync.node_id).to eq('erb_node')
      end
    end
  end

  # -------------------------------------------------------------------------
  # sync_enabled?
  # -------------------------------------------------------------------------
  describe '.sync_enabled?' do
    it 'returns false when node_id is nil' do
      stub_config(node_id: nil, remote_nodes: [remote_node(node_id: 'node_b')])
      expect(PgLwwSync.sync_enabled?).to be false
    end

    it 'returns false when remote_nodes is empty' do
      stub_config(node_id: 'node_a', remote_nodes: [])
      expect(PgLwwSync.sync_enabled?).to be false
    end

    it 'returns true when node_id is set and remote_nodes is non-empty' do
      stub_config(node_id: 'node_a', remote_nodes: [remote_node(node_id: 'node_b')])
      expect(PgLwwSync.sync_enabled?).to be true
    end
  end

  # -------------------------------------------------------------------------
  # assert_valid_configuration!
  # -------------------------------------------------------------------------
  describe '.assert_valid_configuration!' do
    it 'passes when node_id is a valid alphanumeric string' do
      stub_config(node_id: 'us_east_1')
      expect { PgLwwSync.assert_valid_configuration! }.not_to raise_error
    end

    it 'raises when node_id is blank' do
      stub_config(node_id: nil)
      expect { PgLwwSync.assert_valid_configuration! }.to raise_error(RuntimeError, /node_id.*missing/i)
    end

    it 'raises when node_id contains invalid characters' do
      stub_config(node_id: 'node-with-dashes')
      expect { PgLwwSync.assert_valid_configuration! }.to raise_error(RuntimeError, /invalid/i)
    end

    it 'raises when node_id exceeds 64 characters' do
      stub_config(node_id: 'a' * 65)
      expect { PgLwwSync.assert_valid_configuration! }.to raise_error(RuntimeError, /invalid/i)
    end

    it 'accepts node_id at exactly 64 characters' do
      stub_config(node_id: 'a' * 64)
      expect { PgLwwSync.assert_valid_configuration! }.not_to raise_error
    end

    it 'raises when a remote node has an invalid node_id' do
      stub_config(
        node_id:      'local_node',
        remote_nodes: [remote_node(node_id: 'bad-remote!')]
      )
      expect { PgLwwSync.assert_valid_configuration! }.to raise_error(RuntimeError, /invalid/)
    end

    it 'passes when excluded_tables is an array of strings' do
      stub_config(node_id: 'local_node', excluded_tables: ['jobs', 'reporting.cache'])
      expect { PgLwwSync.assert_valid_configuration! }.not_to raise_error
    end

    it 'raises when excluded_tables contains a non-string element' do
      stub_config(node_id: 'local_node', excluded_tables: ['jobs', :cache])
      expect { PgLwwSync.assert_valid_configuration! }.to raise_error(RuntimeError, /excluded_tables/i)
    end
  end

  # -------------------------------------------------------------------------
  # assert_minimum_postgres_version!
  # -------------------------------------------------------------------------
  describe '.assert_minimum_postgres_version!' do
    it 'raises when PostgreSQL version is below 13' do
      allow(ActiveRecord::Base.connection_pool).to receive(:with_connection).and_yield(
        instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter,
                        postgresql_version: 120009)
      )
      expect { PgLwwSync.assert_minimum_postgres_version! }.to raise_error(RuntimeError, /13\.0\+/)
    end

    it 'does not raise when PostgreSQL 13+ is present' do
      allow(ActiveRecord::Base.connection_pool).to receive(:with_connection).and_yield(
        instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter,
                        postgresql_version: 130004)
      )
      expect { PgLwwSync.assert_minimum_postgres_version! }.not_to raise_error
    end
  end

  # -------------------------------------------------------------------------
  # with_primary
  # -------------------------------------------------------------------------
  describe '.with_primary' do
    context 'when sync is disabled' do
      before { stub_config(node_id: nil, remote_nodes: []) }

      it 'yields directly without routing' do
        yielded = false
        PgLwwSync.with_primary { yielded = true }
        expect(yielded).to be true
      end
    end

    context 'when sync is enabled' do
      let(:primary_config) { { node_id: 'node_a', host: 'localhost', port: 5432, database: 'db', username: 'u', password: '' } }

      before do
        stub_config(node_id: 'node_a', remote_nodes: [remote_node(node_id: 'node_b')])
        allow(PgLwwSync::Consensus).to receive(:determine_active_primary).and_return(primary_config)
        allow(PgLwwSync).to receive(:ensure_connection_pool_registered!)
        allow(ActiveRecord::Base).to receive(:connected_to).and_yield
      end

      it 'calls connected_to with the primary node role' do
        expect(ActiveRecord::Base).to receive(:connected_to)
          .with(role: :node_a, prevent_writes: false)
        PgLwwSync.with_primary { }
      end

      it 'yields inside connected_to' do
        yielded = false
        PgLwwSync.with_primary { yielded = true }
        expect(yielded).to be true
      end

      it 'raises NoPrimaryError when consensus fails' do
        allow(PgLwwSync::Consensus).to receive(:determine_active_primary)
          .and_raise(RuntimeError, 'no healthy node')
        expect { PgLwwSync.with_primary { } }.to raise_error(PgLwwSync::NoPrimaryError)
      end
    end
  end

  # -------------------------------------------------------------------------
  # ensure_connection_pool_registered! — regression coverage for
  # connect_timeout/statement_timeout being passed through to
  # establish_connection, so a partitioned/blackholed primary can't hang a
  # live HTTP request indefinitely.
  # -------------------------------------------------------------------------
  describe '.ensure_connection_pool_registered!' do
    let(:node_config) do
      { node_id: 'node_a', host: 'db.internal', port: 5432,
        database: 'app_db', username: 'u', password: 'p' }
    end
    let(:handler) { ActiveRecord::Base.connection_handler }

    before do
      allow(handler).to receive(:retrieve_connection_pool).and_return(nil)
      allow(handler).to receive(:establish_connection)
    end

    it 'sets a default connect_timeout of 5 seconds' do
      expect(handler).to receive(:establish_connection) do |config, **|
        expect(config[:connect_timeout]).to eq(5)
      end
      PgLwwSync.ensure_connection_pool_registered!(node_config)
    end

    it 'sets a default statement_timeout of 30000ms via the variables hash' do
      expect(handler).to receive(:establish_connection) do |config, **|
        expect(config[:variables]).to eq(statement_timeout: 30_000)
      end
      PgLwwSync.ensure_connection_pool_registered!(node_config)
    end

    it 'allows connect_timeout to be overridden per-node' do
      expect(handler).to receive(:establish_connection) do |config, **|
        expect(config[:connect_timeout]).to eq(2)
      end
      PgLwwSync.ensure_connection_pool_registered!(node_config.merge(connect_timeout: 2))
    end

    it 'allows statement_timeout to be overridden per-node' do
      expect(handler).to receive(:establish_connection) do |config, **|
        expect(config[:variables]).to eq(statement_timeout: 5_000)
      end
      PgLwwSync.ensure_connection_pool_registered!(node_config.merge(statement_timeout: 5_000))
    end

    it 'does not re-register a pool that already exists for that role' do
      allow(handler).to receive(:retrieve_connection_pool).and_return(double('pool'))
      expect(handler).not_to receive(:establish_connection)
      PgLwwSync.ensure_connection_pool_registered!(node_config)
    end
  end

  # -------------------------------------------------------------------------
  # on_failure callback
  # -------------------------------------------------------------------------
  describe '.on_failure' do
    it 'stores the block as on_replication_failure' do
      blk = ->(e, ctx) {}
      PgLwwSync.on_failure(&blk)
      expect(PgLwwSync.on_replication_failure).to eq(blk)
    end
  end

  # -------------------------------------------------------------------------
  # NODE_ID_FORMAT constant
  # -------------------------------------------------------------------------
  describe 'NODE_ID_FORMAT' do
    valid_ids   = %w[node1 us_east US_EAST node_123 A]
    invalid_ids = ['node-1', 'node.1', 'node 1', '', 'a' * 65, 'node!']

    valid_ids.each do |id|
      it "accepts #{id.inspect}" do
        expect(id).to match(PgLwwSync::NODE_ID_FORMAT)
      end
    end

    invalid_ids.each do |id|
      it "rejects #{id.inspect}" do
        expect(id).not_to match(PgLwwSync::NODE_ID_FORMAT)
      end
    end
  end
end
