require 'yaml'
require 'erb'
require_relative "pg_lww_sync/version"
require_relative "pg_lww_sync/railtie" if defined?(Rails)

module PgLwwSync
  SYSTEM_SCHEMAS = %w[pg_catalog information_schema pg_lww_sync pg_toast].freeze
  SYSTEM_TABLES  = %w[schema_migrations ar_internal_metadata].freeze
  TARGET_CHANGESET_TABLE = "pg_lww_sync.pg_lww_changesets"

  # node_id must be safe to embed in SQL identifiers / JSON keys
  NODE_ID_FORMAT = /\A[a-z0-9_]{1,64}\z/i
  NoPrimaryError = Class.new(StandardError)
  # Shared mutex for thread-safe connection pool registration, used by both
  # RequestRouter middleware and with_primary.
  POOL_REGISTRATION_MUTEX = Mutex.new

  class << self
    attr_reader :on_replication_failure

    def config
      @config ||= load_config_file!
    end

    def node_id
      config[:node_id]
    end

    def remote_nodes
      config[:remote_nodes] || []
    end

    def on_failure(&block)
      @on_replication_failure = block
    end

    # Routes the block's database writes to the consensus-elected primary node.
    # All nodes in the cluster share the same primary connection, so advisory
    # locks and other coordination primitives work correctly across workers,
    # console sessions, and any other non-HTTP code path that needs distributed
    # mutual exclusion.
    #
    # When sync is disabled (no node_id / no remote_nodes), yields directly
    # without any connection switching — the local database is the only one.
    #
    # Usage:
    #   PgLwwSync.with_primary do
    #     # All AR calls here go to the elected primary
    #     ActiveRecord::Base.connection.execute("SELECT pg_advisory_lock(42)")
    #     MyModel.create!(...)
    #   end
    #
    # Raises PgLwwSync::NoPrimaryError if no healthy primary can be elected.
    def with_primary(&block)
      unless sync_enabled?
        return yield
      end

      primary_config = PgLwwSync::Consensus.determine_active_primary
      ensure_connection_pool_registered!(primary_config)

      ActiveRecord::Base.connected_to(
        role:           primary_config[:node_id].to_sym,
        prevent_writes: false,
        &block
      )
    rescue => e
      raise PgLwwSync::NoPrimaryError, "Failed to connect to primary: #{e.message}"
    end

    # Registers an AR connection pool for a node config if one doesn't exist yet.
    # Shared by RequestRouter middleware and with_primary so pool registration
    # logic is defined once. Thread-safe via PgLwwSync::POOL_REGISTRATION_MUTEX.
    def ensure_connection_pool_registered!(node_config)
      role = node_config[:node_id].to_sym

      return if ActiveRecord::Base.connection_handler.retrieve_connection_pool(
        "ActiveRecord::Base", role: role
      )

      PgLwwSync::POOL_REGISTRATION_MUTEX.synchronize do
        return if ActiveRecord::Base.connection_handler.retrieve_connection_pool(
          "ActiveRecord::Base", role: role
        )

        ActiveRecord::Base.connection_handler.establish_connection(
          {
            adapter:  node_config[:adapter]  || "postgresql",
            host:     node_config[:host],
            port:     node_config[:port]     || 5432,
            database: node_config[:database],
            username: node_config[:username],
            password: node_config[:password],
            pool:     node_config[:pool]     || 5
          },
          owner_name: "ActiveRecord::Base",
          role:       role
        )
      end
    end

    def sync_enabled?
      node_id.present? && remote_nodes.size > 0
    end

    def assert_valid_configuration!
      if node_id.blank?
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          'node_id' is missing or unassigned for the '#{Rails.env}' environment profile.
          Verify your config/pg_lww_sync.yml mapping details.
        ERROR
      end

      unless node_id.match?(NODE_ID_FORMAT)
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          'node_id' value #{node_id.inspect} is invalid.
          Only alphanumeric characters and underscores are allowed (max 64 chars).
        ERROR
      end

      remote_nodes.each do |node|
        unless node[:node_id].to_s.match?(NODE_ID_FORMAT)
          raise "[PgLwwSync] CONFIGURATION ERROR: remote node_id #{node[:node_id].inspect} is invalid."
        end
      end
      
      node_ids = remote_nodes.map { |n| n[:node_id].to_s }
      if node_ids.uniq.size != node_ids.size
        raise "[PgLwwSync] CONFIGURATION ERROR: duplicate node_id detected in remote_nodes."
      end

      if node_ids.include?(node_id.to_s)
        raise "[PgLwwSync] CONFIGURATION ERROR: local node_id #{node_id.inspect} " \
              "also appears in remote_nodes. Each node must have a unique identifier."
      end
    end

    def assert_minimum_postgres_version!
      ActiveRecord::Base.connection_pool.with_connection do |conn|
        if conn.postgresql_version < 130000
          raise "[PgLwwSync Fatal Error] Incompatible Engine! PostgreSQL 13.0+ required."
        end
      end
    end

    # Attaches or drops the sync trigger on every eligible table depending on
    # whether sync is enabled. Each table is handled in its own execute call so
    # we never hold DDL locks on multiple tables simultaneously — critical in
    # production with many tables.
    #
    # Only tables with a single UUID primary key column named 'id' are eligible.
    # Composite PKs, non-UUID PKs, differently-named PKs, partitioned tables, and
    # foreign tables are all skipped with a warning.
    #
    # When sync is disabled, drops triggers and truncates the outbox and
    # stale changesets accumulate.
    def sync_all_tables!
      conn = ActiveRecord::Base.connection

      if sync_enabled?
        assert_valid_configuration!
      end


      system_schemas_list = SYSTEM_SCHEMAS.map { |s| conn.quote(s) }.join(", ")
      system_tables_list  = SYSTEM_TABLES.map  { |t| conn.quote(t) }.join(", ")

      tables = conn.select_rows(<<~SQL)
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN (#{system_schemas_list})
          AND table_name   NOT IN (#{system_tables_list})
        ORDER BY table_schema, table_name;
      SQL

      tables.each do |schema, table|
        # Properly build a schema-qualified identifier using separate quoting
        qualified_table = "#{conn.quote_table_name(schema)}.#{conn.quote_table_name(table)}"

        # Skip partitioned tables
        is_partitioned = conn.select_value(<<~SQL)
          SELECT EXISTS(
            SELECT 1 FROM pg_partitioned_table
            WHERE partrelid = #{conn.quote("#{schema}.#{table}")}::regclass
          )
        SQL

        if is_partitioned
          Rails.logger.info "[PgLwwSync] Skipping partitioned table: #{schema}.#{table}"
          next
        end

        # Skip foreign tables
        is_foreign = conn.select_value(<<~SQL)
          SELECT EXISTS(
            SELECT 1 FROM pg_foreign_table ft
            JOIN pg_class c ON ft.ftrelid = c.oid
            WHERE c.oid = #{conn.quote("#{schema}.#{table}")}::regclass
          )
        SQL

        if is_foreign
          Rails.logger.info "[PgLwwSync] Skipping foreign table: #{schema}.#{table}"
          next
        end

        # Only sync tables with a single-column UUID primary key named 'id'.
        # Composite PKs, non-UUID PKs, and differently-named PKs are skipped.
        has_uuid_id_pk = conn.select_value(<<~SQL)
          SELECT EXISTS(
            SELECT 1
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = #{conn.quote("#{schema}.#{table}")}::regclass
              AND i.indisprimary
              AND array_length(i.indkey, 1) = 1
              AND a.attname = 'id'
              AND a.atttypid = 'uuid'::regtype
          )
        SQL

        unless has_uuid_id_pk
          Rails.logger.warn "[PgLwwSync] Skipping #{schema}.#{table} — requires a single UUID primary key column named 'id'"
          next
        end

        if sync_enabled?
          # Ensure __lww_ts column exists — stores per-column write keys inline
          # on the row so timing data and row data are always atomic and travel
          # together in snapshots without any separate table to manage.
          conn.execute("ALTER TABLE #{qualified_table} ADD COLUMN IF NOT EXISTS __lww_ts JSONB NOT NULL DEFAULT '{}'::jsonb;")
          conn.execute(<<~SQL)
            DROP TRIGGER IF EXISTS c_pg_lww_sync_tg ON #{qualified_table};
            CREATE TRIGGER c_pg_lww_sync_tg
            BEFORE INSERT OR UPDATE OR DELETE ON #{qualified_table}
            FOR EACH ROW EXECUTE PROCEDURE pg_lww_sync.log_lww_sync_changeset();
          SQL
        else
          conn.execute("DROP TRIGGER IF EXISTS c_pg_lww_sync_tg ON #{qualified_table};")
        end
      end

      unless sync_enabled?
        conn.execute("TRUNCATE #{TARGET_CHANGESET_TABLE};")
        Rails.logger.info "[PgLwwSync] Sync disabled — triggers dropped and outbox truncated."
      end
    end


    def initialize_database_functions!
      assert_valid_configuration!

      quoted_node_id = ActiveRecord::Base.connection.quote(PgLwwSync.node_id)


      # Only local_node_id() is recreated on boot — it embeds the node_id
      # from config so the trigger and apply_lww_change know which node they
      # are running on. The trigger function and apply_lww_change are static
      # SQL installed once by the migration.
      ActiveRecord::Base.connection.execute(<<~SQL)
        CREATE OR REPLACE FUNCTION pg_lww_sync.local_node_id()
        RETURNS CHARACTER VARYING AS $$
        BEGIN
          RETURN #{quoted_node_id}::character varying;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
      SQL
    end

    private

    def load_config_file!
      config_path = Rails.root.join("config", "pg_lww_sync.yml")

      unless File.exist?(config_path)
        raise <<~ERROR

          [PgLwwSync] CONFIGURATION ERROR:
          Config file not found at config/pg_lww_sync.yml.
          Run: rails generate pg_lww_sync:install
        ERROR
      end

      raw_yaml = File.read(config_path)
      compiled_yaml = ERB.new(raw_yaml).result
      full_config = YAML.safe_load(compiled_yaml, aliases: true) || {}
      env_config = full_config[Rails.env] || {}

      {
        node_id:                env_config["node_id"],
        remote_nodes:           (env_config["remote_nodes"] || []).map(&:symbolize_keys),
        worker_threads:         env_config["worker_threads"],
        prune_retain_days:      env_config["prune_retain_days"],
        prune_interval_seconds: env_config["prune_interval_seconds"]
      }
    end
  end
end

require_relative "pg_lww_sync/consensus"
require_relative "pg_lww_sync/middleware"
require_relative "pg_lww_sync/replication_client"
require_relative "pg_lww_sync/consumer"
