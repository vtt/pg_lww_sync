module ConfigHelpers
  def stub_config(node_id: 'test_node', remote_nodes: [])
    PgLwwSync.instance_variable_set(:@config, {
      node_id:      node_id,
      remote_nodes: remote_nodes
    })
  end

  def remote_node(node_id:, **overrides)
    {
      node_id:  node_id,
      host:     'localhost',
      port:     5432,
      database: 'pg_lww_sync_test',
      username: ENV.fetch('PGUSER', 'postgres'),
      password: ENV.fetch('PGPASSWORD', ''),
      pool:     3
    }.merge(overrides)
  end
end

RSpec.configure do |config|
  config.include ConfigHelpers
end
