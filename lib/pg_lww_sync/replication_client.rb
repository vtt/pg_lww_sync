module PgLwwSync
  class ReplicationClient
    @pools = {}
    @mutex = Mutex.new

    def self.connect_to(node_config)
      node_id = node_config[:node_id]
      pool    = fetch_or_create_pool(node_id, node_config)

      pool.connection_pool.with_connection do |conn|
        yield conn
      end
    rescue ActiveRecord::ConnectionNotEstablished, PG::Error => e
      Rails.logger.error "[PgLwwSync ReplicationClient] Connection failed for '#{node_id}': #{e.message}"
      # Evict the broken pool so it is rebuilt on the next attempt
      @mutex.synchronize { @pools.delete(node_id) }
      raise
    end

    private

    def self.fetch_or_create_pool(node_id, node_config)
      # Fast path: pool already exists
      return @pools[node_id] if @pools[node_id]

      # Slow path: create the pool under a mutex to prevent duplicate establishment
      @mutex.synchronize do
        @pools[node_id] ||= establish_connection(node_config)
      end
    end

    def self.establish_connection(node_config)
      # Anonymous subclass isolates this connection from the main AR connection handler
      connection_class = Class.new(ActiveRecord::Base)
      connection_class.establish_connection(
        adapter:  node_config[:adapter]  || 'postgresql',
        host:     node_config[:host],
        username: node_config[:username],
        password: node_config[:password],
        database: node_config[:database],
        port:     node_config[:port]     || 5432,
        pool:     node_config[:pool]     || 5,
        # Without these, a partitioned/blackholed peer (packets silently
        # dropped rather than refused) can hang a worker thread for the OS's
        # TCP connect timeout (often minutes), or for as long as a wedged
        # remote query takes. Since a worker processes one whole chain —
        # including every configured remote node, sequentially — before
        # returning to the queue, that can tie up the entire worker pool and
        # stall delivery to every other healthy peer too, not just this one.
        connect_timeout: node_config[:connect_timeout] || 5,
        variables: { statement_timeout: node_config[:statement_timeout] || 30_000 }
      )
      connection_class
    end
  end
end
