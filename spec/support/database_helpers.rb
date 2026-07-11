module DatabaseHelpers
  # ---------------------------------------------------------------------------
  # Schema bootstrap — creates the pg_lww_sync schema and outbox table.
  # __lww_ts is an inline JSONB column on application tables — no column_timings.
  # ---------------------------------------------------------------------------
  def bootstrap_pg_lww_sync_schema!
    conn = ActiveRecord::Base.connection
    conn.execute("CREATE SCHEMA IF NOT EXISTS pg_lww_sync;")
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pg_lww_sync.pg_lww_changesets (
        id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        table_schema     TEXT        NOT NULL,
        table_name       TEXT        NOT NULL,
        record_id        TEXT        NOT NULL,
        action_type      TEXT        NOT NULL,
        changed_fields   JSONB       NOT NULL,
        column_timestamps JSONB      NOT NULL DEFAULT '{}',
        transaction_id   BIGINT      NOT NULL,
        committed_at     TIMESTAMPTZ NOT NULL,
        processed_nodes  JSONB       NOT NULL DEFAULT '{}',
        status           TEXT        NOT NULL DEFAULT 'pending'
      );
    SQL
    conn.execute(<<~SQL)
      CREATE INDEX IF NOT EXISTS idx_lww_changesets_status_committed
        ON pg_lww_sync.pg_lww_changesets (status, committed_at);
    SQL
  end

  def drop_pg_lww_sync_schema!
    ActiveRecord::Base.connection.execute("DROP SCHEMA IF EXISTS pg_lww_sync CASCADE;")
  end

  def clean_pg_lww_sync_tables!
    conn = ActiveRecord::Base.connection
    conn.execute("TRUNCATE pg_lww_sync.pg_lww_changesets;")
  end

  # ---------------------------------------------------------------------------
  # Test table helpers — UUID PK named 'id' + __lww_ts JSONB column
  # ---------------------------------------------------------------------------
  def create_test_table!(name: 'lww_test_widgets', schema: 'public')
    conn = ActiveRecord::Base.connection
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS #{schema}.#{name} (
        id        UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
        name      TEXT,
        quantity  INTEGER DEFAULT 0,
        __lww_ts  JSONB   NOT NULL DEFAULT '{}'
      );
    SQL
  end

  def drop_test_table!(name: 'lww_test_widgets', schema: 'public')
    ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS #{schema}.#{name} CASCADE;")
  end

  # ---------------------------------------------------------------------------
  # Outbox helpers
  # ---------------------------------------------------------------------------
  def insert_changeset!(
    table_schema: 'public',
    table_name:   'lww_test_widgets',
    record_id:    SecureRandom.uuid,
    action_type:  'UPDATE',
    changed_fields: '{"name":"test"}',
    column_timestamps: '{"name":"001700000000.000000_node_b"}',
    transaction_id: 1,
    committed_at: Time.now.utc,
    processed_nodes: '{}',
    status: 'pending'
  )
    conn = ActiveRecord::Base.connection
    conn.execute(<<~SQL)
      INSERT INTO pg_lww_sync.pg_lww_changesets
        (table_schema, table_name, record_id, action_type,
         changed_fields, column_timestamps, transaction_id,
         committed_at, processed_nodes, status)
      VALUES (
        #{conn.quote(table_schema)}, #{conn.quote(table_name)},
        #{conn.quote(record_id)}, #{conn.quote(action_type)},
        #{conn.quote(changed_fields)}::jsonb,
        #{conn.quote(column_timestamps)}::jsonb,
        #{transaction_id.to_i},
        #{conn.quote(committed_at.iso8601(6))},
        #{conn.quote(processed_nodes)}::jsonb,
        #{conn.quote(status)}
      );
    SQL
  end

  # ---------------------------------------------------------------------------
  # Write key helper — builds a composite write key matching the trigger format.
  # "000000000000.000000_node_id" — zero-padded epoch + underscore + node_id
  # ---------------------------------------------------------------------------
  def write_key(time: Time.now.utc, node_id: 'node_a')
    format('%012.6f_%s', time.to_f, node_id)
  end

  # ---------------------------------------------------------------------------
  # __lww_ts helpers — read/write the inline timing column on application rows
  # ---------------------------------------------------------------------------
  def set_lww_ts(table:, record_id:, ts_map:, schema: 'public')
    conn = ActiveRecord::Base.connection
    conn.execute(<<~SQL)
      UPDATE #{schema}.#{table}
      SET __lww_ts = #{conn.quote(ts_map.to_json)}::jsonb
      WHERE id = #{conn.quote(record_id)};
    SQL
  end

  def get_lww_ts(table:, record_id:, schema: 'public')
    conn = ActiveRecord::Base.connection
    raw = conn.select_value("SELECT __lww_ts FROM #{schema}.#{table} WHERE id = #{conn.quote(record_id)};")
    raw ? JSON.parse(raw) : {}
  end

  # ---------------------------------------------------------------------------
  # Count helpers
  # ---------------------------------------------------------------------------
  def changeset_count(status: nil)
    conn = ActiveRecord::Base.connection
    sql  = "SELECT COUNT(*) FROM pg_lww_sync.pg_lww_changesets"
    sql += " WHERE status = #{conn.quote(status)}" if status
    conn.select_value(sql).to_i
  end

  # No column_timing_count — column_timings table no longer exists.
  # Use get_lww_ts to inspect per-column write keys inline on the row.
end

RSpec.configure do |config|
  config.include DatabaseHelpers
end
