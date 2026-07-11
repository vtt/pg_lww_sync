module PgLwwSyncHelpers
  def mock_ar_connection(overrides = {})
    conn = instance_double(
      ActiveRecord::ConnectionAdapters::PostgreSQLAdapter,
      {
        quote:              ->(v) { "'#{v}'" },
        quote_table_name:   ->(v) { "\"#{v}\"" },
        execute:            double("PGResult", cmd_tuples: 0),
        select_value:       nil,
        select_values:      [],
        select_all:         double("ActiveRecord::Result", to_a: []),
        select_rows:        [],
        postgresql_version: 140000
      }.merge(overrides)
    )
    allow(conn).to receive(:quote) { |v| "'#{v}'" }
    conn
  end

  # Returns a Hash representing one outbox row as select_all would return it.
  # record_id is now a plain UUID text string (not a JSON array).
  def changeset_row(overrides = {})
    {
      "id"                => SecureRandom.uuid,
      "table_schema"      => "public",
      "table_name"        => "orders",
      "record_id"         => SecureRandom.uuid,
      "action_type"       => "UPDATE",
      "changed_fields"    => '{"amount":99}',
      "column_timestamps" => '{"amount":"001704067200.000000_node_b"}',
      "transaction_id"    => "12345",
      "committed_at"      => 10.minutes.ago.iso8601,
      "processed_nodes"   => "{}",
      "status"            => "pending"
    }.merge(overrides)
  end

  def stub_rails_logger
    logger = instance_double(Logger, info: nil, warn: nil, error: nil, debug: nil)
    allow(Rails).to receive(:logger).and_return(logger)
    logger
  end
end

RSpec.configure do |config|
  config.include PgLwwSyncHelpers
end
