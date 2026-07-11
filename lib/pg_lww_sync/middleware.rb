module PgLwwSync
  class RequestRouter
    # Class-level mutex so pool registration is serialised across all instances
    # and all threads, regardless of how many times Rack instantiates this class.

    # HTTP methods that are safe to retry because they carry no mutation intent.
    # POST/PATCH/PUT/DELETE must NOT be retried: if the connection drops after the
    # database commits but before the response is sent, a retry would re-execute
    # the controller action and produce a duplicate write + a second outbox entry.
    IDEMPOTENT_METHODS = %w[GET HEAD OPTIONS].freeze

    def initialize(app)
      @app = app
    end

    def call(env)
      blacklisted_node_ids = []
      retries = 0
      max_retries = PgLwwSync.remote_nodes.size + 1
      primary_node_config = nil
      # Capture the HTTP method once, before any retry, so downstream middleware
      # cannot mutate env while we loop.
      http_method = env["REQUEST_METHOD"].to_s.upcase

      begin
        primary_node_config = PgLwwSync::Consensus.determine_active_primary(blacklist: blacklisted_node_ids)
        ensure_pool_registered!(primary_node_config)

        ActiveRecord::Base.connected_to(role: primary_node_config[:node_id].to_sym, prevent_writes: false) do
          @app.call(env)
        end

      rescue PG::ConnectionBad, ActiveRecord::ConnectionNotEstablished => e
        # These errors fire before any SQL executes (connection establishment failed), so retrying on any HTTP method is safe.
        if primary_node_config && retries < max_retries
          Rails.logger.error "[PgLwwSync Middleware] Node #{primary_node_config[:node_id]} failed: #{e.message}"
          blacklisted_node_ids << primary_node_config[:node_id]
          retries += 1
          PgLwwSync::Consensus.invalidate_fast_path!
          retry
        else
          raise
        end

      rescue ActiveRecord::StatementInvalid => e
        # StatementInvalid can fire mid-transaction — after writes have been sent
        # to the database and potentially committed. Only retry idempotent methods
        # to prevent duplicate mutations and duplicate outbox entries.
        if primary_node_config && retries < max_retries && IDEMPOTENT_METHODS.include?(http_method)
          Rails.logger.error "[PgLwwSync Middleware] Node #{primary_node_config[:node_id]} statement error on #{http_method}: #{e.message}"
          blacklisted_node_ids << primary_node_config[:node_id]
          retries += 1
          PgLwwSync::Consensus.invalidate_fast_path!
          retry
        else
          Rails.logger.error "[PgLwwSync Middleware] Not retrying #{http_method} after StatementInvalid — mutation may have committed." unless IDEMPOTENT_METHODS.include?(http_method)
          raise
        end
      end
    end

    private

    def ensure_pool_registered!(node_config)
      PgLwwSync.ensure_connection_pool_registered!(node_config)
    end
  end
end
