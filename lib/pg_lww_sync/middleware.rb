module PgLwwSync
  class RequestRouter
    # Class-level mutex so pool registration is serialised across all instances
    # and all threads, regardless of how many times Rack instantiates this class.

    # HTTP methods that are safe to retry because they carry no mutation intent.
    # POST/PATCH/PUT/DELETE must NOT be retried on an error where a write may
    # already have reached the database: a retry would re-execute the
    # controller action and produce a duplicate write + a second outbox entry.
    IDEMPOTENT_METHODS = %w[GET HEAD OPTIONS].freeze

    # Errors that mean "a write may have already been sent to (and possibly
    # committed on) the database" — safe to retry only for idempotent methods.
    #
    # ActiveRecord::ConnectionFailed doesn't exist on every Rails version this
    # gem supports (>= 6.1) — it's a comparatively recent addition, only
    # raised on Rails versions whose PostgreSQL adapter distinguishes a
    # mid-query connection failure (query may have already run) from a
    # pre-query one (see ActiveRecord::ConnectionNotEstablished below).
    # Referencing an undefined constant directly in a `rescue` clause raises
    # NameError the moment any exception hits this method — Ruby resolves the
    # rescue class list lazily, at dispatch time — which would mask the real
    # error on older Rails. Build the list defensively once, at load time,
    # instead.
    AMBIGUOUS_CONNECTION_ERRORS = [PG::ConnectionBad, ActiveRecord::StatementInvalid]
    AMBIGUOUS_CONNECTION_ERRORS << ActiveRecord::ConnectionFailed if defined?(ActiveRecord::ConnectionFailed)
    AMBIGUOUS_CONNECTION_ERRORS.freeze

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

      rescue ActiveRecord::ConnectionNotEstablished => e
        # Rails only raises this specific class when it's certain the query
        # was never sent to the server — either the connection was never
        # established at all, or (on Rails versions whose adapter makes the
        # distinction) a pg-gem-internal PG::ConnectionBad that failed before
        # managing to send the query. Genuinely safe to retry on any method.
        if primary_node_config && retries < max_retries
          Rails.logger.error "[PgLwwSync Middleware] Node #{primary_node_config[:node_id]} failed: #{e.message}"
          blacklisted_node_ids << primary_node_config[:node_id]
          retries += 1
          PgLwwSync::Consensus.invalidate_fast_path!
          retry
        else
          raise
        end

      rescue *AMBIGUOUS_CONNECTION_ERRORS => e
        # A raw PG::ConnectionBad, an ActiveRecord::StatementInvalid, or an
        # ActiveRecord::ConnectionFailed can all fire mid-transaction — after
        # a write has been sent to the database and potentially committed.
        # Only retry idempotent methods to prevent duplicate mutations and
        # duplicate outbox entries.
        if primary_node_config && retries < max_retries && IDEMPOTENT_METHODS.include?(http_method)
          Rails.logger.error "[PgLwwSync Middleware] Node #{primary_node_config[:node_id]} error on #{http_method}: #{e.class}: #{e.message}"
          blacklisted_node_ids << primary_node_config[:node_id]
          retries += 1
          PgLwwSync::Consensus.invalidate_fast_path!
          retry
        else
          Rails.logger.error "[PgLwwSync Middleware] Not retrying #{http_method} after #{e.class} — mutation may have committed." unless IDEMPOTENT_METHODS.include?(http_method)
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
