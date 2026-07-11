require 'spec_helper'

RSpec.describe PgLwwSync::Consumer do
  before { stub_config(node_id: 'node_a', remote_nodes: []) }

  let(:consumer) { described_class.new(batch_size: 10, sleep_interval: 0.01, pool_size: 2) }

  # -------------------------------------------------------------------------
  # Constants
  # -------------------------------------------------------------------------
  describe 'retention constants' do
    it 'keeps changesets for 7 days' do
      expect(described_class::PRUNE_RETAIN_DAYS).to eq(7)
    end
    # column_timings table removed — write keys stored inline in __lww_ts
    # on each application row; no separate retention policy needed.
  end

  # -------------------------------------------------------------------------
  # stop!
  # -------------------------------------------------------------------------
  describe '#stop!' do
    it 'sets @shutdown to true' do
      consumer.stop!
      expect(consumer.instance_variable_get(:@shutdown)).to be true
    end
  end

  # -------------------------------------------------------------------------
  # process_next_replication_batch!
  # -------------------------------------------------------------------------
  describe '#process_next_replication_batch!' do
    context 'when there are no pending changesets' do
      before { allow(consumer).to receive(:fetch_pending_outbox_changesets).and_return([]) }

      it 'returns without pushing anything to the queue' do
        consumer.process_next_replication_batch!
        expect(consumer.instance_variable_get(:@work_queue).size).to eq(0)
      end
    end

    context 'when changesets are present' do
      let(:rows) do
        [
          { 'id' => SecureRandom.uuid, 'transaction_id' => '42' },
          { 'id' => SecureRandom.uuid, 'transaction_id' => '42' }
        ]
      end

      before { allow(consumer).to receive(:fetch_pending_outbox_changesets).and_return(rows) }

      it 'groups rows by transaction_id and pushes one item per transaction' do
        consumer.process_next_replication_batch!
        expect(consumer.instance_variable_get(:@work_queue).size).to eq(1)
      end

      it 'pushes the correct transaction_id and rows' do
        consumer.process_next_replication_batch!
        tx_id, tx_rows = consumer.instance_variable_get(:@work_queue).pop(true)
        expect(tx_id).to eq('42')
        expect(tx_rows.size).to eq(2)
      end
    end
  end

  # -------------------------------------------------------------------------
  # process_transaction_bucket
  # -------------------------------------------------------------------------
  describe '#process_transaction_bucket (private)' do
    let(:node_b) { remote_node(node_id: 'node_b') }

    context 'when all rows are already marked delivered for the target node' do
      let(:rows) do
        [{ 'id' => SecureRandom.uuid,
           'processed_nodes' => '{"node_b":"delivered"}' }]
      end

      before { stub_config(node_id: 'node_a', remote_nodes: [node_b]) }

      it 'skips replication to that node' do
        expect(PgLwwSync::ReplicationClient).not_to receive(:connect_to)
        consumer.send(:process_transaction_bucket, 42, rows)
      end
    end

    context 'when delivery fails with a transient error' do
      let(:rows) do
        [{ 'id' => SecureRandom.uuid, 'processed_nodes' => '{}',
           'table_schema' => 'public', 'table_name' => 'widgets',
           'record_id' => SecureRandom.uuid, 'action_type' => 'UPDATE',
           'transaction_id' => 42, 'changed_fields' => '{}',
           'column_timestamps' => '{}' }]
      end

      before do
        stub_config(node_id: 'node_a', remote_nodes: [node_b])
        allow(PgLwwSync::ReplicationClient).to receive(:connect_to)
          .and_raise(PG::ConnectionBad, 'connection refused')
        allow(consumer).to receive(:update_local_node_status)
      end

      it 'does not quarantine — leaves status pending for retry' do
        expect(consumer).not_to receive(:quarantine_poison_pill_rows)
        consumer.send(:process_transaction_bucket, 42, rows)
      end

      it 'logs the transient failure' do
        expect(Rails.logger).to receive(:error).with(/Transient failure/)
        consumer.send(:process_transaction_bucket, 42, rows)
      end
    end

    context 'when delivery fails with a schema error (poison pill)' do
      let(:row_id) { SecureRandom.uuid }
      let(:rows) do
        [{ 'id' => row_id, 'processed_nodes' => '{}',
           'table_schema' => 'public', 'table_name' => 'widgets',
           'record_id' => SecureRandom.uuid, 'action_type' => 'UPDATE',
           'transaction_id' => 42, 'changed_fields' => '{}',
           'column_timestamps' => '{}' }]
      end

      before do
        stub_config(node_id: 'node_a', remote_nodes: [node_b])
        remote_conn = instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
        allow(remote_conn).to receive(:transaction).and_raise(
          ActiveRecord::StatementInvalid.new('column "missing" does not exist')
        )
        allow(PgLwwSync::ReplicationClient).to receive(:connect_to).and_yield(remote_conn)
        allow(consumer).to receive(:update_local_node_status)
        allow(consumer).to receive(:quarantine_poison_pill_rows)
      end

      it 'quarantines only the failing row by id' do
        expect(consumer).to receive(:quarantine_poison_pill_rows)
          .with([row_id], 'node_b')
        consumer.send(:process_transaction_bucket, 42, rows)
      end

      it 'logs the poison pill failure' do
        expect(Rails.logger).to receive(:error).with(/quarantined as poison pill/)
        consumer.send(:process_transaction_bucket, 42, rows)
      end

      it 'does not raise' do
        expect { consumer.send(:process_transaction_bucket, 42, rows) }.not_to raise_error
      end
    end
  end

  # -------------------------------------------------------------------------
  # transient_error?
  # -------------------------------------------------------------------------
  describe '#transient_error? (private)' do
    it 'returns true for PG::ConnectionBad' do
      expect(consumer.send(:transient_error?, PG::ConnectionBad.new('gone'))).to be true
    end

    it 'returns true for PG::UnableToSend' do
      expect(consumer.send(:transient_error?, PG::UnableToSend.new('socket'))).to be true
    end

    it 'returns true for ActiveRecord::ConnectionNotEstablished' do
      expect(consumer.send(:transient_error?, ActiveRecord::ConnectionNotEstablished.new)).to be true
    end

    it 'returns false for ActiveRecord::StatementInvalid (schema error)' do
      expect(consumer.send(:transient_error?,
        ActiveRecord::StatementInvalid.new('column "x" does not exist'))).to be false
    end
  end

  # -------------------------------------------------------------------------
  # format_lww_sql_call
  # -------------------------------------------------------------------------
  describe '#format_lww_sql_call (private)' do
    let(:record_id) { SecureRandom.uuid }
    let(:row) do
      {
        'table_schema'      => 'public',
        'table_name'        => 'widgets',
        'record_id'         => record_id,
        'action_type'       => 'UPDATE',
        'changed_fields'    => '{"name":"foo"}',
        'column_timestamps' => '{"name":"001704067200.000000_node_b"}'
      }
    end

    subject(:sql) { consumer.send(:format_lww_sql_call, row, 'node_b') }

    it 'calls pg_lww_sync.apply_lww_change' do
      expect(sql).to include('pg_lww_sync.apply_lww_change')
    end

    it 'passes record_id as plain TEXT (not ::jsonb)' do
      expect(sql).to include(conn_quote(record_id))
      expect(sql).not_to match(/::jsonb/)
    end

    it 'passes PgLwwSync.node_id as origin (not from row)' do
      expect(sql).to include("'node_a'")
    end

    def conn_quote(v)
      ActiveRecord::Base.connection.quote(v)
    end
  end

  # -------------------------------------------------------------------------
  # parse_processed_nodes
  # -------------------------------------------------------------------------
  describe '#parse_processed_nodes (private)' do
    it 'returns an empty hash for nil' do
      expect(consumer.send(:parse_processed_nodes, nil)).to eq({})
    end

    it 'returns the hash as-is when already a Hash' do
      h = { 'node_b' => 'delivered' }
      expect(consumer.send(:parse_processed_nodes, h)).to eq(h)
    end

    it 'parses a JSON string into a hash' do
      expect(consumer.send(:parse_processed_nodes, '{"node_b":"delivered"}')).to eq('node_b' => 'delivered')
    end
  end

  # -------------------------------------------------------------------------
  # update_local_node_status — operates on row IDs not transaction_id
  # -------------------------------------------------------------------------
  describe '#update_local_node_status (private)' do
    let(:conn) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }

    before do
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
      allow(conn).to receive(:quote) { |v| "'#{v}'" }
      allow(conn).to receive(:execute)
    end

    it 'updates by row id IN clause, not transaction_id' do
      row_ids = [SecureRandom.uuid, SecureRandom.uuid]
      expect(conn).to receive(:execute).with(/WHERE id IN/) do |sql|
        row_ids.each { |id| expect(sql).to include(id) }
      end.at_least(:once)
      consumer.send(:update_local_node_status, row_ids, 'node_b', 'delivered')
    end

    it 'uses delivered (not success) as the delivery state' do
      expect(conn).to receive(:execute).with(/'delivered'/) do |sql|
        sql
      end.at_least(:once)
      consumer.send(:update_local_node_status, [SecureRandom.uuid], 'node_b', 'delivered')
    end

    it 'is a no-op for empty row_ids' do
      expect(conn).not_to receive(:execute)
      consumer.send(:update_local_node_status, [], 'node_b', 'delivered')
    end
  end

  # -------------------------------------------------------------------------
  # quarantine_poison_pill_rows — scoped by row id
  # -------------------------------------------------------------------------
  describe '#quarantine_poison_pill_rows (private)' do
    let(:conn) { instance_double(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) }

    before do
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
      allow(conn).to receive(:quote) { |v| "'#{v}'" }
    end

    it 'marks specific row ids as failed for the target node' do
      row_ids = [SecureRandom.uuid]
      expect(conn).to receive(:execute) do |sql|
        expect(sql).to include("'failed'")
        expect(sql).to include("WHERE id IN")
        expect(sql).to include(row_ids.first)
      end
      consumer.send(:quarantine_poison_pill_rows, row_ids, 'node_b')
    end

    it 'does NOT set row-level status to failed' do
      expect(conn).to receive(:execute) do |sql|
        expect(sql).not_to match(/SET status/)
      end
      consumer.send(:quarantine_poison_pill_rows, [SecureRandom.uuid], 'node_b')
    end

    it 'is a no-op for empty row_ids' do
      expect(conn).not_to receive(:execute)
      consumer.send(:quarantine_poison_pill_rows, [], 'node_b')
    end
  end

  # -------------------------------------------------------------------------
  # Worker lifecycle
  # -------------------------------------------------------------------------
  describe '#reap_and_respawn_workers (private)' do
    it 'replaces dead workers to keep pool at pool_size' do
      consumer.instance_variable_get(:@pool_size).times do
        consumer.send(:spawn_and_register_worker)
      end

      workers = consumer.instance_variable_get(:@workers)
      workers.first.kill
      sleep 0.05

      consumer.send(:reap_and_respawn_workers)
      sleep 0.05

      live = consumer.instance_variable_get(:@workers).select(&:alive?)
      expect(live.size).to eq(consumer.instance_variable_get(:@pool_size))
    ensure
      consumer.stop!
      consumer.instance_variable_get(:@workers).each { |t| t.kill rescue nil }
    end
  end

  # -------------------------------------------------------------------------
  # Shutdown
  # -------------------------------------------------------------------------
  describe '#drain_and_shutdown_workers (private)' do
    it 'closes the queue and joins all workers within the grace period' do
      consumer.instance_variable_get(:@pool_size).times do
        consumer.send(:spawn_and_register_worker)
      end
      consumer.stop!
      expect { consumer.send(:drain_and_shutdown_workers) }.not_to raise_error
      expect(consumer.instance_variable_get(:@workers).none?(&:alive?)).to be true
    end
  end
end
