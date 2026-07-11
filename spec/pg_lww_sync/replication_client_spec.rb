require 'spec_helper'

RSpec.describe PgLwwSync::ReplicationClient do
  before do
    stub_config(node_id: 'node_a', remote_nodes: [remote_node(node_id: 'node_b')])
    described_class.instance_variable_set(:@pools, {})
    described_class.instance_variable_set(:@mutex, Mutex.new)
  end

  let(:node_config) { remote_node(node_id: 'node_b') }
  let(:mock_conn)   { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }
  let(:mock_pool)   { instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool) }
  let(:mock_class) do
    klass = Class.new(ActiveRecord::Base)
    allow(klass).to receive(:connection_pool).and_return(mock_pool)
    klass
  end

  before { allow(mock_pool).to receive(:with_connection).and_yield(mock_conn) }

  # -------------------------------------------------------------------------
  # connect_to
  # -------------------------------------------------------------------------
  describe '.connect_to' do
    it 'yields the connection to the block' do
      allow(described_class).to receive(:fetch_or_create_pool).and_return(mock_class)
      yielded = nil
      described_class.connect_to(node_config) { |c| yielded = c }
      expect(yielded).to eq(mock_conn)
    end

    it 'evicts the pool on PG::Error so the next call rebuilds it' do
      allow(described_class).to receive(:fetch_or_create_pool).and_return(mock_class)
      allow(mock_pool).to receive(:with_connection).and_raise(PG::Error, 'connection lost')

      described_class.connect_to(node_config) { } rescue nil

      pools = described_class.instance_variable_get(:@pools)
      expect(pools['node_b']).to be_nil
    end

    it 're-raises PG::Error after evicting the pool' do
      allow(described_class).to receive(:fetch_or_create_pool).and_return(mock_class)
      allow(mock_pool).to receive(:with_connection).and_raise(PG::Error, 'connection lost')

      expect { described_class.connect_to(node_config) { } }.to raise_error(PG::Error)
    end

    it 'does not evict the pool on non-PG errors' do
      allow(described_class).to receive(:fetch_or_create_pool).and_return(mock_class)
      allow(mock_pool).to receive(:with_connection).and_raise(StandardError, 'app error')

      described_class.connect_to(node_config) { } rescue nil

      pools = described_class.instance_variable_get(:@pools)
      expect(pools['node_b']).not_to be_nil
    end
  end

  # -------------------------------------------------------------------------
  # fetch_or_create_pool (private)
  # -------------------------------------------------------------------------
  describe '.fetch_or_create_pool (private)' do
    it 'creates a pool on the first call for a node' do
      allow(described_class).to receive(:establish_connection).and_return(mock_class)
      described_class.send(:fetch_or_create_pool, 'node_b', node_config)
      expect(described_class).to have_received(:establish_connection).once
    end

    it 'returns the cached pool on subsequent calls' do
      allow(described_class).to receive(:establish_connection).and_return(mock_class)
      described_class.send(:fetch_or_create_pool, 'node_b', node_config)
      described_class.send(:fetch_or_create_pool, 'node_b', node_config)
      expect(described_class).to have_received(:establish_connection).once
    end

    it 'creates separate pools for different nodes' do
      allow(described_class).to receive(:establish_connection).and_return(mock_class)
      described_class.send(:fetch_or_create_pool, 'node_b', node_config)
      described_class.send(:fetch_or_create_pool, 'node_c', remote_node(node_id: 'node_c'))
      expect(described_class).to have_received(:establish_connection).twice
    end
  end

  # -------------------------------------------------------------------------
  # establish_connection (private)
  # -------------------------------------------------------------------------
  describe '.establish_connection (private)' do
    it 'returns an anonymous ActiveRecord::Base subclass' do
      result = described_class.send(:establish_connection, node_config)
      expect(result.ancestors).to include(ActiveRecord::Base)
      expect(result).not_to eq(ActiveRecord::Base)
    end

    it 'defaults port to 5432 when not specified' do
      config = node_config.reject { |k, _| k == :port }
      klass  = described_class.send(:establish_connection, config)
      expect(klass.connection_db_config.configuration_hash[:port]).to eq(5432)
    end

    it 'defaults pool to 5 when not specified' do
      config = node_config.reject { |k, _| k == :pool }
      klass  = described_class.send(:establish_connection, config)
      expect(klass.connection_db_config.configuration_hash[:pool]).to eq(5)
    end
  end
end
