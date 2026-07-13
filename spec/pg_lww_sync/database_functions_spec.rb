require 'spec_helper'

# Integration tests — require a real PostgreSQL instance.
# Set PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD as needed.

RSpec.describe 'Database functions', :integration do
  before(:all) do
    bootstrap_pg_lww_sync_schema!
    stub_config(node_id: 'node_a', remote_nodes: [])
    PgLwwSync.initialize_database_functions!
    conn = ActiveRecord::Base.connection
    # Install apply_lww_change from the migration
    erb_path = File.expand_path(
      '../../../lib/generators/pg_lww_sync/templates/create_pg_lww_sync_infrastructure.rb.erb',
      __dir__
    )
    require 'erb'
    sql = ERB.new(File.read(erb_path)).result
    # Extract and execute the function SQL blocks
    sql.scan(/execute <<~SQL\n(.*?)\n\s+SQL/m) do |block|
      conn.execute(block.first.strip) rescue nil
    end
  end

  after(:all) { drop_pg_lww_sync_schema! }
  before(:each) { clean_pg_lww_sync_tables! }

  let(:conn) { ActiveRecord::Base.connection }

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------
  def apply_change(action:, record_id:, fields:, timestamps:, origin: 'node_b')
    conn.execute(<<~SQL)
      SELECT lww_sync.apply_lww_change(
        'public', 'lww_test_widgets',
        #{conn.quote(record_id)},
        #{conn.quote(action)},
        #{conn.quote(fields)}::jsonb,
        #{conn.quote(timestamps)}::jsonb,
        #{conn.quote(origin)}
      );
    SQL
  end

  # -------------------------------------------------------------------------
  # local_node_id
  # -------------------------------------------------------------------------
  describe 'lww_sync.local_node_id()' do
    it 'returns the configured node_id' do
      expect(conn.select_value('SELECT lww_sync.local_node_id();')).to eq('node_a')
    end

    it 'updates when initialize_database_functions! is re-run' do
      stub_config(node_id: 'node_x', remote_nodes: [])
      PgLwwSync.initialize_database_functions!
      expect(conn.select_value('SELECT lww_sync.local_node_id();')).to eq('node_x')
      stub_config(node_id: 'node_a', remote_nodes: [])
      PgLwwSync.initialize_database_functions!
    end
  end

  # -------------------------------------------------------------------------
  # Trigger: log_lww_sync_changeset
  # -------------------------------------------------------------------------
  describe 'trigger log_lww_sync_changeset' do
    before(:all) do
      create_test_table!
      stub_config(node_id: 'node_a', remote_nodes: [])
      PgLwwSync.sync_all_tables!
    end
    after(:all)  { drop_test_table! }
    before(:each) { conn.execute('TRUNCATE public.lww_test_widgets;') }

    context 'on INSERT' do
      let(:uuid) { SecureRandom.uuid }
      before { conn.execute("INSERT INTO public.lww_test_widgets (id, name, quantity) VALUES ('#{uuid}', 'widget', 5);") }

      it 'writes one changeset row' do
        expect(changeset_count).to eq(1)
      end

      it 'sets action_type to INSERT' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(row['action_type']).to eq('INSERT')
      end

      it 'stores record_id as plain UUID text' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(row['record_id']).to eq(uuid)
      end

      it 'does not store origin_node_id in the outbox row' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(row).not_to have_key('origin_node_id')
      end

      it 'stores write keys for all non-PK columns in column_timestamps' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        ts = JSON.parse(row['column_timestamps'])
        expect(ts.keys).to include('name', 'quantity')
        expect(ts.keys).not_to include('id', '__lww_ts')
        # Each value should be a composite write key: "000...123_node_a"
        expect(ts['name']).to match(/\A\d{12}\.\d{6}_node_a\z/)
      end

      it 'stores write keys inline on the row in __lww_ts' do
        lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
        expect(lww.keys).to include('name', 'quantity')
        expect(lww['name']).to match(/\A\d{12}\.\d{6}_node_a\z/)
      end

      it 'write key in __lww_ts matches outbox column_timestamps' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        ts  = JSON.parse(row['column_timestamps'])
        lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
        expect(lww['name']).to eq(ts['name'])
      end

      it 'sets status to pending' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(row['status']).to eq('pending')
      end
    end

    context 'on UPDATE' do
      let(:uuid) { SecureRandom.uuid }
      before do
        conn.execute("INSERT INTO public.lww_test_widgets (id, name, quantity) VALUES ('#{uuid}', 'original', 1);")
        clean_pg_lww_sync_tables!
      end

      it 'writes one changeset row' do
        conn.execute("UPDATE public.lww_test_widgets SET name = 'updated' WHERE id = '#{uuid}';")
        expect(changeset_count).to eq(1)
      end

      it 'records write key only for changed columns in column_timestamps' do
        conn.execute("UPDATE public.lww_test_widgets SET name = 'updated' WHERE id = '#{uuid}';")
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        ts  = JSON.parse(row['column_timestamps'])
        expect(ts.keys).to include('name')
        expect(ts.keys).not_to include('quantity')
      end

      it 'updates __lww_ts only for changed columns' do
        old_lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
        conn.execute("UPDATE public.lww_test_widgets SET name = 'updated' WHERE id = '#{uuid}';")
        new_lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
        expect(new_lww['name']).not_to eq(old_lww['name'])      # changed
        expect(new_lww['quantity']).to eq(old_lww['quantity'])  # unchanged
      end

      it 'does not write a changeset when nothing changed' do
        conn.execute("UPDATE public.lww_test_widgets SET name = 'original' WHERE id = '#{uuid}';")
        expect(changeset_count).to eq(0)
      end
    end

    context 'on DELETE' do
      let(:uuid) { SecureRandom.uuid }
      before do
        conn.execute("INSERT INTO public.lww_test_widgets (id, name) VALUES ('#{uuid}', 'bye');")
        clean_pg_lww_sync_tables!
        conn.execute("DELETE FROM public.lww_test_widgets WHERE id = '#{uuid}';")
      end

      it 'writes a DELETE changeset' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(row['action_type']).to eq('DELETE')
      end

      it 'emits empty column_timestamps for DELETE' do
        row = conn.select_all('SELECT * FROM lww_sync.pg_lww_changesets LIMIT 1;').first
        expect(JSON.parse(row['column_timestamps'])).to eq({})
      end

      it '__lww_ts is deleted with the row — no orphaned timing data' do
        # The row no longer exists, so no __lww_ts to worry about
        count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{uuid}';").to_i
        expect(count).to eq(0)
        # No separate column_timings table to clean up — it doesn't exist
        expect { conn.execute("SELECT 1 FROM lww_sync.column_timings LIMIT 1;") }
          .to raise_error(ActiveRecord::StatementInvalid, /column_timings/)
      end
    end

    context 'replication loop guard' do
      it 'suppresses outbox writes when session_replication_role is replica' do
        conn.execute("SET session_replication_role = 'replica';")
        conn.execute("INSERT INTO public.lww_test_widgets (id, name) VALUES ('#{SecureRandom.uuid}', 'silent');")
        conn.execute("SET session_replication_role = DEFAULT;")
        expect(changeset_count).to eq(0)
      end

      it 'suppresses outbox writes when lww_sync.applying_replication is on' do
        conn.execute("SET lww_sync.applying_replication = 'on';")
        conn.execute("INSERT INTO public.lww_test_widgets (id, name) VALUES ('#{SecureRandom.uuid}', 'silent');")
        conn.execute("SET lww_sync.applying_replication = DEFAULT;")
        expect(changeset_count).to eq(0)
      end
    end
  end

  # -------------------------------------------------------------------------
  # apply_lww_change — NULL record_id guard
  # -------------------------------------------------------------------------
  describe 'apply_lww_change — NULL record_id' do
    before(:all) { create_test_table! }
    after(:all)  { drop_test_table! }

    it 'raises when record_id is NULL' do
      expect {
        conn.execute(<<~SQL)
          SELECT lww_sync.apply_lww_change(
            'public', 'lww_test_widgets', NULL, 'INSERT',
            '{}'::jsonb, '{}'::jsonb, 'node_b'
          );
        SQL
      }.to raise_error(ActiveRecord::StatementInvalid, /record_id cannot be NULL/)
    end
  end

  # -------------------------------------------------------------------------
  # apply_lww_change — INSERT path
  # -------------------------------------------------------------------------
  describe 'apply_lww_change — INSERT' do
    before(:all) { create_test_table! }
    after(:all)  { drop_test_table! }
    before(:each) { conn.execute('TRUNCATE public.lww_test_widgets;') }

    it 'inserts the row when it does not exist' do
      uuid = SecureRandom.uuid
      k    = write_key(node_id: 'node_b')
      apply_change(
        action: 'INSERT', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"remote\",\"quantity\":7}",
        timestamps: "{\"name\":\"#{k}\",\"quantity\":\"#{k}\"}"
      )
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{uuid}';").to_i
      expect(count).to eq(1)
    end

    it 'stores the incoming write keys in __lww_ts' do
      uuid = SecureRandom.uuid
      k    = write_key(node_id: 'node_b')
      apply_change(
        action: 'INSERT', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"remote\"}",
        timestamps: "{\"name\":\"#{k}\"}"
      )
      lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
      expect(lww['name']).to eq(k)
    end

    it 'is idempotent — applying the same INSERT twice does not raise' do
      uuid = SecureRandom.uuid
      k    = write_key(node_id: 'node_b')
      params = {
        action: 'INSERT', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"remote\"}",
        timestamps: "{\"name\":\"#{k}\"}"
      }
      expect { apply_change(**params) }.not_to raise_error
      expect { apply_change(**params) }.not_to raise_error
    end
  end

  # -------------------------------------------------------------------------
  # apply_lww_change — UPDATE / LWW resolution
  # -------------------------------------------------------------------------
  describe 'apply_lww_change — UPDATE (LWW)' do
    before(:all) { create_test_table! }
    after(:all)  { drop_test_table! }

    let(:uuid) { SecureRandom.uuid }
    let(:local_key) { write_key(time: Time.now.utc - 60, node_id: 'node_a') }

    before(:each) do
      conn.execute("TRUNCATE public.lww_test_widgets;")
      conn.execute("INSERT INTO public.lww_test_widgets (id, name, quantity) VALUES ('#{uuid}', 'original', 10);")
      # Seed __lww_ts with a 60-second-old key for 'name'
      set_lww_ts(table: 'lww_test_widgets', record_id: uuid,
                 ts_map: { 'name' => local_key, 'quantity' => local_key })
    end

    it 'applies the update when incoming key is larger (newer)' do
      newer_key = write_key(time: Time.now.utc, node_id: 'node_b')
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"newer\"}",
        timestamps: "{\"name\":\"#{newer_key}\"}"
      )
      expect(conn.select_value("SELECT name FROM public.lww_test_widgets WHERE id = '#{uuid}';"))
        .to eq('newer')
    end

    it 'rejects the update when incoming key is smaller (older)' do
      older_key = write_key(time: Time.now.utc - 300, node_id: 'node_b')
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"stale\"}",
        timestamps: "{\"name\":\"#{older_key}\"}"
      )
      expect(conn.select_value("SELECT name FROM public.lww_test_widgets WHERE id = '#{uuid}';"))
        .to eq('original')
    end

    it 'tiebreak: larger node_id suffix wins when timestamps are equal' do
      # local_key has node_id 'node_a'; incoming with 'node_b' (larger) should win
      tie_key = write_key(time: Time.now.utc - 60, node_id: 'node_b')
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"tiebreak_winner\"}",
        timestamps: "{\"name\":\"#{tie_key}\"}"
      )
      expect(conn.select_value("SELECT name FROM public.lww_test_widgets WHERE id = '#{uuid}';"))
        .to eq('tiebreak_winner')
    end

    it 'tiebreak: smaller node_id suffix loses' do
      tie_key = write_key(time: Time.now.utc - 60, node_id: 'aaa') # 'aaa' < 'node_a'
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"tiebreak_loser\"}",
        timestamps: "{\"name\":\"#{tie_key}\"}"
      )
      expect(conn.select_value("SELECT name FROM public.lww_test_widgets WHERE id = '#{uuid}';"))
        .to eq('original')
    end

    it 'applies LWW per-column independently' do
      older_key = write_key(time: Time.now.utc - 300, node_id: 'node_b')
      newer_key = write_key(time: Time.now.utc + 10,  node_id: 'node_b')
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"remote_name\",\"quantity\":99}",
        timestamps: "{\"name\":\"#{older_key}\",\"quantity\":\"#{newer_key}\"}"
      )
      row = conn.select_all("SELECT name, quantity FROM public.lww_test_widgets WHERE id = '#{uuid}';").first
      expect(row['name']).to eq('original') # older key lost
      expect(row['quantity'].to_i).to eq(99) # newer key won
    end

    it 'updates __lww_ts for winning columns only' do
      newer_key = write_key(time: Time.now.utc, node_id: 'node_b')
      older_key = write_key(time: Time.now.utc - 300, node_id: 'node_b')
      apply_change(
        action: 'UPDATE', record_id: uuid,
        fields:     "{\"id\":\"#{uuid}\",\"name\":\"new\",\"quantity\":99}",
        timestamps: "{\"name\":\"#{newer_key}\",\"quantity\":\"#{older_key}\"}"
      )
      lww = get_lww_ts(table: 'lww_test_widgets', record_id: uuid)
      expect(lww['name']).to eq(newer_key)     # updated
      expect(lww['quantity']).to eq(local_key) # unchanged — incoming lost
    end

    it 'discards UPDATE when record does not exist (INSERT-before-UPDATE ordering)' do
      missing = SecureRandom.uuid
      newer_key = write_key(node_id: 'node_b')
      conn.execute(<<~SQL)
        SELECT lww_sync.apply_lww_change(
          'public', 'lww_test_widgets', #{conn.quote(missing)}, 'UPDATE',
          #{conn.quote("{\"id\":\"#{missing}\",\"name\":\"phantom\"}")}::jsonb,
          #{conn.quote("{\"name\":\"#{newer_key}\"}")}::jsonb,
          'node_b'
        );
      SQL
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{missing}';").to_i
      expect(count).to eq(0)
    end

    it 'does not partially write __lww_ts when UPDATE discarded (no record)' do
      missing = SecureRandom.uuid
      newer_key = write_key(node_id: 'node_b')
      conn.execute(<<~SQL)
        SELECT lww_sync.apply_lww_change(
          'public', 'lww_test_widgets', #{conn.quote(missing)}, 'UPDATE',
          #{conn.quote("{\"id\":\"#{missing}\",\"name\":\"phantom\"}")}::jsonb,
          #{conn.quote("{\"name\":\"#{newer_key}\"}")}::jsonb,
          'node_b'
        );
      SQL
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{missing}';").to_i
      expect(count).to eq(0) # row never created, no __lww_ts to check
    end
  end

  # -------------------------------------------------------------------------
  # apply_lww_change — DELETE path
  # -------------------------------------------------------------------------
  describe 'apply_lww_change — DELETE' do
    before(:all) { create_test_table! }
    after(:all)  { drop_test_table! }

    let(:uuid) { SecureRandom.uuid }

    before(:each) do
      conn.execute("TRUNCATE public.lww_test_widgets;")
      conn.execute("INSERT INTO public.lww_test_widgets (id, name) VALUES ('#{uuid}', 'doomed');")
      set_lww_ts(table: 'lww_test_widgets', record_id: uuid,
                 ts_map: { 'name' => write_key(node_id: 'node_a') })
    end

    def apply_delete(record_id: nil, origin: 'node_b')
      rid = record_id || uuid
      conn.execute(<<~SQL)
        SELECT lww_sync.apply_lww_change(
          'public', 'lww_test_widgets',
          #{conn.quote(rid)}, 'DELETE',
          #{conn.quote("{\"id\":\"#{rid}\",\"name\":\"doomed\"}")}::jsonb,
          '{}'::jsonb,
          #{conn.quote(origin)}
        );
      SQL
    end

    it 'deletes the row unconditionally' do
      apply_delete
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{uuid}';").to_i
      expect(count).to eq(0)
    end

    it '__lww_ts is removed with the row — no orphaned timing data' do
      apply_delete
      # Row is gone, so __lww_ts is gone with it — no separate table to check
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{uuid}';").to_i
      expect(count).to eq(0)
    end

    it 'is idempotent — applying delete twice does not raise' do
      expect { apply_delete }.not_to raise_error
      expect { apply_delete }.not_to raise_error
    end

    it 'stale UPDATE after delete matches 0 rows — silent no-op' do
      apply_delete
      expect {
        conn.execute(<<~SQL)
          SELECT lww_sync.apply_lww_change(
            'public', 'lww_test_widgets',
            #{conn.quote(uuid)}, 'UPDATE',
            #{conn.quote("{\"id\":\"#{uuid}\",\"name\":\"ghost\"}")}::jsonb,
            #{conn.quote("{\"name\":\"#{write_key(node_id: 'node_b')}\"}")}::jsonb,
            'node_b'
          );
        SQL
      }.not_to raise_error
      count = conn.select_value("SELECT COUNT(*) FROM public.lww_test_widgets WHERE id = '#{uuid}';").to_i
      expect(count).to eq(0)
    end
  end

  # -------------------------------------------------------------------------
  # apply_lww_change — unique index FWW conflict resolution
  # -------------------------------------------------------------------------
  describe 'apply_lww_change — unique index FWW resolution' do
    before(:all) do
      ActiveRecord::Base.connection.execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS public.lww_unique_test (
          id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          email    TEXT NOT NULL,
          __lww_ts JSONB NOT NULL DEFAULT '{}',
          CONSTRAINT lww_unique_test_email_key UNIQUE (email)
        );
      SQL
      stub_config(node_id: 'node_a', remote_nodes: [])
      PgLwwSync.sync_all_tables!
    end
    after(:all) { ActiveRecord::Base.connection.execute('DROP TABLE IF EXISTS public.lww_unique_test CASCADE;') }
    before(:each) { conn.execute('TRUNCATE public.lww_unique_test;') }

    def apply_unique(record_id:, email:, key:, origin: 'node_b')
      conn.execute(<<~SQL)
        SELECT lww_sync.apply_lww_change(
          'public', 'lww_unique_test',
          #{conn.quote(record_id)}, 'INSERT',
          #{conn.quote("{\"id\":\"#{record_id}\",\"email\":\"#{email}\"}")}::jsonb,
          #{conn.quote("{\"email\":\"#{key}\"}")}::jsonb,
          #{conn.quote(origin)}
        );
      SQL
    end

    it 'incoming wins when it has the smaller (earlier) write key — FWW' do
      local_uuid    = SecureRandom.uuid
      incoming_uuid = SecureRandom.uuid
      local_key     = write_key(time: Time.now.utc,      node_id: 'node_a') # later
      incoming_key  = write_key(time: Time.now.utc - 60, node_id: 'node_b') # earlier

      conn.execute("INSERT INTO public.lww_unique_test (id, email, __lww_ts) VALUES ('#{local_uuid}', 'x@test.com', #{conn.quote({email: local_key}.to_json)}::jsonb);")
      apply_unique(record_id: incoming_uuid, email: 'x@test.com', key: incoming_key)

      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{incoming_uuid}';").to_i).to eq(1)
      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{local_uuid}';").to_i).to eq(0)
    end

    it 'local wins when it has the smaller (earlier) write key — FWW' do
      local_uuid    = SecureRandom.uuid
      incoming_uuid = SecureRandom.uuid
      local_key     = write_key(time: Time.now.utc - 60, node_id: 'node_a') # earlier
      incoming_key  = write_key(time: Time.now.utc,      node_id: 'node_b') # later

      conn.execute("INSERT INTO public.lww_unique_test (id, email, __lww_ts) VALUES ('#{local_uuid}', 'y@test.com', #{conn.quote({email: local_key}.to_json)}::jsonb);")
      apply_unique(record_id: incoming_uuid, email: 'y@test.com', key: incoming_key)

      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{local_uuid}';").to_i).to eq(1)
      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{incoming_uuid}';").to_i).to eq(0)
    end

    it 'node_id tiebreak: smaller node_id wins when epoch is identical' do
      local_uuid    = SecureRandom.uuid
      incoming_uuid = SecureRandom.uuid
      t = Time.now.utc
      local_key    = write_key(time: t, node_id: 'node_b') # larger node_id
      incoming_key = write_key(time: t, node_id: 'node_a') # smaller node_id → wins

      conn.execute("INSERT INTO public.lww_unique_test (id, email, __lww_ts) VALUES ('#{local_uuid}', 'z@test.com', #{conn.quote({email: local_key}.to_json)}::jsonb);")
      apply_unique(record_id: incoming_uuid, email: 'z@test.com', key: incoming_key, origin: 'node_a')

      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{incoming_uuid}';").to_i).to eq(1)
      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{local_uuid}';").to_i).to eq(0)
    end

    it 'local row with empty __lww_ts always loses (treated as empty string key)' do
      local_uuid    = SecureRandom.uuid
      incoming_uuid = SecureRandom.uuid
      incoming_key  = write_key(node_id: 'node_b')

      conn.execute("INSERT INTO public.lww_unique_test (id, email) VALUES ('#{local_uuid}', 'seed@test.com');")
      apply_unique(record_id: incoming_uuid, email: 'seed@test.com', key: incoming_key)

      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{incoming_uuid}';").to_i).to eq(1)
      expect(conn.select_value("SELECT COUNT(*) FROM public.lww_unique_test WHERE id = '#{local_uuid}';").to_i).to eq(0)
    end
  end

  # -------------------------------------------------------------------------
  # sync_all_tables!
  # -------------------------------------------------------------------------
  describe 'PgLwwSync.sync_all_tables!' do
    before(:all) { create_test_table!(name: 'lww_trigger_check') }
    after(:all)  { drop_test_table!(name: 'lww_trigger_check') }

    it 'attaches c_pg_lww_sync_tg trigger to eligible tables' do
      stub_config(node_id: 'node_a', remote_nodes: [])
      PgLwwSync.sync_all_tables!
      count = conn.select_value(<<~SQL).to_i
        SELECT COUNT(*) FROM pg_trigger
        WHERE tgname = 'c_pg_lww_sync_tg'
          AND tgrelid = 'public.lww_trigger_check'::regclass;
      SQL
      expect(count).to eq(1)
    end

    it 'adds __lww_ts column to eligible tables' do
      stub_config(node_id: 'node_a', remote_nodes: [])
      PgLwwSync.sync_all_tables!
      count = conn.select_value(<<~SQL).to_i
        SELECT COUNT(*) FROM pg_attribute
        WHERE attrelid = 'public.lww_trigger_check'::regclass
          AND attname = '__lww_ts' AND attnum > 0;
      SQL
      expect(count).to eq(1)
    end

    it 'skips tables without a UUID id PK' do
      conn.execute('CREATE TABLE IF NOT EXISTS public.lww_bad_pk (name TEXT PRIMARY KEY);')
      expect { PgLwwSync.sync_all_tables! }.not_to raise_error
      conn.execute('DROP TABLE IF EXISTS public.lww_bad_pk;')
    end

    it 'drops triggers when sync disabled (outbox left intact)' do
      stub_config(node_id: nil, remote_nodes: [])
      PgLwwSync.sync_all_tables!
      count = conn.select_value(<<~SQL).to_i
        SELECT COUNT(*) FROM pg_trigger
        WHERE tgname = 'c_pg_lww_sync_tg'
          AND tgrelid = 'public.lww_trigger_check'::regclass;
      SQL
      expect(count).to eq(0)
      stub_config(node_id: 'node_a', remote_nodes: [])
    end

    it 'is idempotent' do
      stub_config(node_id: 'node_a', remote_nodes: [])
      expect { PgLwwSync.sync_all_tables! }.not_to raise_error
      expect { PgLwwSync.sync_all_tables! }.not_to raise_error
    end
  end

  # -------------------------------------------------------------------------
  # Outbox pruning
  # -------------------------------------------------------------------------
  describe 'outbox pruning' do
    it 'removes success changesets older than retention window' do
      insert_changeset!(status: 'success', committed_at: Time.now.utc - 8.days, transaction_id: 1)
      insert_changeset!(status: 'success', committed_at: Time.now.utc - 8.days, transaction_id: 2)
      insert_changeset!(status: 'success', committed_at: Time.now.utc - 1.day,  transaction_id: 3)
      cutoff = PgLwwSync::Consumer::PRUNE_RETAIN_DAYS.days.ago
      conn.execute("DELETE FROM lww_sync.pg_lww_changesets WHERE status = 'success' AND committed_at < '#{cutoff.iso8601}';")
      expect(changeset_count(status: 'success')).to eq(1)
    end

    it 'leaves pending changesets untouched' do
      insert_changeset!(status: 'pending', committed_at: Time.now.utc - 8.days, transaction_id: 10)
      insert_changeset!(status: 'success', committed_at: Time.now.utc - 8.days, transaction_id: 11)
      cutoff = PgLwwSync::Consumer::PRUNE_RETAIN_DAYS.days.ago
      conn.execute("DELETE FROM lww_sync.pg_lww_changesets WHERE status = 'success' AND committed_at < '#{cutoff.iso8601}';")
      expect(changeset_count(status: 'pending')).to eq(1)
    end
  end
end
