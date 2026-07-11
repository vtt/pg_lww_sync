require 'spec_helper'
require 'tmpdir'

RSpec.describe PgLwwSync::Consensus do
  before { stub_config(node_id: 'node_a') }

  describe '.state_file' do
    it 'includes the node_id in the filename' do
      expect(described_class.state_file).to include('node_a')
    end

    it 'uses /dev/shm when available' do
      allow(File).to receive(:directory?).with('/dev/shm').and_return(true)
      expect(described_class.state_file).to start_with('/dev/shm/')
    end

    it 'falls back to Dir.tmpdir when /dev/shm is absent' do
      allow(File).to receive(:directory?).with('/dev/shm').and_return(false)
      expect(described_class.state_file).to start_with(Dir.tmpdir)
    end
  end

  describe '.determine_active_primary — fast path' do
    around do |example|
      Dir.mktmpdir do |dir|
        state_path = File.join(dir, "pg_lww_sync_node_a.json")
        allow(described_class).to receive(:state_file).and_return(state_path)
        example.run
      end
    end

    it 'returns the cached primary when the tick matches and node is not blacklisted' do
      current_tick = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i / 5
      state = { tick: current_tick, primary: { node_id: 'node_a', host: 'localhost' } }
      File.write(described_class.state_file, state.to_json)

      result = described_class.determine_active_primary
      expect(result[:node_id]).to eq('node_a')
    end

    it 'bypasses the fast path when the cached primary is blacklisted' do
      current_tick = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i / 5
      state = { tick: current_tick, primary: { node_id: 'node_a', host: 'localhost' } }
      File.write(described_class.state_file, state.to_json)

      allow(described_class).to receive(:perform_network_health_checks)
        .and_return({ node_id: 'node_b', host: 'localhost' })

      result = described_class.determine_active_primary(blacklist: ['node_a'])
      expect(result[:node_id]).to eq('node_b')
    end

    it 'falls through when state file is missing' do
      allow(described_class).to receive(:perform_network_health_checks)
        .and_return({ node_id: 'node_a' })
      expect { described_class.determine_active_primary }.not_to raise_error
    end

    it 'falls through when state file is corrupt JSON' do
      File.write(described_class.state_file, 'not valid json{{{')
      allow(described_class).to receive(:perform_network_health_checks)
        .and_return({ node_id: 'node_a' })
      expect { described_class.determine_active_primary }.not_to raise_error
    end

    it 'falls through when the tick is stale' do
      stale_tick = (Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i / 5) - 2
      state = { tick: stale_tick, primary: { node_id: 'node_a' } }
      File.write(described_class.state_file, state.to_json)

      allow(described_class).to receive(:perform_network_health_checks)
        .and_return({ node_id: 'node_a' })

      described_class.determine_active_primary
      expect(described_class).to have_received(:perform_network_health_checks)
    end
  end

  describe '.invalidate_fast_path!' do
    around do |example|
      Dir.mktmpdir do |dir|
        state_path = File.join(dir, "pg_lww_sync_node_a.json")
        allow(described_class).to receive(:state_file).and_return(state_path)
        example.run
      end
    end

    it 'writes tick: 0 so the next caller falls through to a health check' do
      File.write(described_class.state_file, '{"tick":999,"primary":{"node_id":"node_a"}}')
      described_class.invalidate_fast_path!
      data = JSON.parse(File.read(described_class.state_file))
      expect(data['tick']).to eq(0)
    end

    it 'does not raise when the state file does not exist' do
      expect { described_class.invalidate_fast_path! }.not_to raise_error
    end
  end

  describe '.perform_network_health_checks (private)' do
    let(:pg_conn)   { instance_double(PG::Connection) }
    let(:pg_result) { instance_double(PG::Result) }

    before do
      allow(PG).to receive(:connect).and_return(pg_conn)
      allow(pg_conn).to receive(:exec).and_return(pg_result)
      allow(pg_conn).to receive(:close)
      allow(pg_result).to receive(:getvalue).with(0, 0).and_return('f')
    end

    it 'sets a 2-second statement timeout before health check query' do
      stub_config(node_id: 'node_a', remote_nodes: [remote_node(node_id: 'node_b')])
      expect(pg_conn).to receive(:exec).with("SET statement_timeout = '2s';").at_least(:once)
      described_class.send(:perform_network_health_checks, blacklist: [])
    end

    it 'returns the node with the lowest node_id when multiple nodes are healthy' do
      stub_config(
        node_id:      'node_b',
        remote_nodes: [remote_node(node_id: 'node_a'), remote_node(node_id: 'node_c')]
      )
      result = described_class.send(:perform_network_health_checks, blacklist: [])
      expect(result[:node_id]).to eq('node_a')
    end

    it 'raises when all nodes are in recovery or unreachable' do
      allow(pg_result).to receive(:getvalue).with(0, 0).and_return('t')
      expect {
        described_class.send(:perform_network_health_checks, blacklist: [])
      }.to raise_error(RuntimeError, /No responsive primary/)
    end

    it 'raises when all nodes are blacklisted' do
      stub_config(node_id: 'node_a', remote_nodes: [])
      expect {
        described_class.send(:perform_network_health_checks, blacklist: ['node_a'])
      }.to raise_error(RuntimeError, /All nodes are blacklisted/)
    end
  end

  describe '.read_json (private)' do
    it 'returns nil for an empty file' do
      f = StringIO.new('')
      expect(described_class.send(:read_json, f)).to be_nil
    end

    it 'returns nil for a whitespace-only file' do
      f = StringIO.new("   \n\t  ")
      expect(described_class.send(:read_json, f)).to be_nil
    end

    it 'parses valid JSON with symbolized names' do
      f = StringIO.new('{"tick":5,"primary":{"node_id":"node_a"}}')
      result = described_class.send(:read_json, f)
      expect(result[:tick]).to eq(5)
      expect(result[:primary][:node_id]).to eq('node_a')
    end
  end
end
