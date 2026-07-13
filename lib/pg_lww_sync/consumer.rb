require 'json'

module PgLwwSync
  class Consumer
    MAX_HORIZON_AGE_SECONDS  = 60
    HORIZON_CACHE_TTL        = 10
    # Outbox changesets are safe to drop once delivered; 7 days covers ops windows.
    DEFAULT_PRUNE_RETAIN_DAYS      = 7
    DEFAULT_PRUNE_INTERVAL_SECONDS = 3600

    QUEUE_PUSH_TIMEOUT       = 5
    # How long stop! waits for in-flight worker jobs to finish before giving up.
    GRACEFUL_SHUTDOWN_TIMEOUT = 30

    def initialize(batch_size: 250, sleep_interval: 5, pool_size: 3,
                   prune_retain_days: DEFAULT_PRUNE_RETAIN_DAYS,
                   prune_interval_seconds: DEFAULT_PRUNE_INTERVAL_SECONDS)
      @batch_size             = batch_size
      @sleep_interval         = sleep_interval
      @pool_size              = pool_size
      @prune_retain_days      = prune_retain_days
      @prune_interval_seconds = prune_interval_seconds
      @last_pruned_at         = nil
      @iteration_count = 0

      # Monotonic clock reference point for horizon TTL — immune to wall-clock
      # adjustments from DST changes or NTP corrections.
      @last_horizon_check_mono = nil
      @cached_horizon_blocked  = false

      @local_db_mutex = Mutex.new
      @work_queue     = SizedQueue.new(@pool_size * 4)

      # Written by stop! (from a signal handler or Puma hook); read by the main
      # loop and workers. Declared before spawn_worker so threads see it immediately.
      @shutdown = false

      # Worker registry — maintained by the main loop so dead workers can be
      # detected and respawned without restarting the entire consumer.
      @workers      = []
      @workers_mutex = Mutex.new
    end

    def start!
      Rails.logger.info "[PgLwwSync Consumer] Spinning up with #{@pool_size} worker thread(s)."

      @pool_size.times { spawn_and_register_worker }

      until @shutdown
        begin
          # Periodically check for dead workers and respawn them so the pool
          # stays at @pool_size regardless of unhandled exceptions or JVM GC issues.
          reap_and_respawn_workers

          process_next_replication_batch!
          @iteration_count += 1
          if @last_pruned_at.nil? || (monotonic_now - @last_pruned_at) >= @prune_interval_seconds
            prune_delivered_changesets!
            @last_pruned_at = monotonic_now
          end
        rescue => e
          Rails.logger.error "[PgLwwSync Consumer] Main loop error: #{e.class}: #{e.message}"
          sleep 2
          next
        end

        sleep @sleep_interval
      end

      drain_and_shutdown_workers
    end

    # Called by the signal handler or Puma phased-restart hook.
    # Safe to call from any thread.
    def stop!
      Rails.logger.info "[PgLwwSync Consumer] Shutdown requested — draining in-flight work."
      @shutdown = true
    end

    def process_next_replication_batch!
      rows = fetch_pending_outbox_changesets
      return if rows.empty?

      # Group rows by transaction_id, then detect ordering dependencies:
      # any two transactions sharing a record_id must be delivered in
      # committed_at order by the same worker — otherwise an UPDATE processed
      # before its INSERT would find no existing row, be discarded, and the
      # UPDATE data would be permanently lost even though the INSERT arrives
      # moments later on a different worker thread.
      #
      # Algorithm: build dependency chains — if transaction A and transaction B
      # both touch record X, merge them into one ordered chain. Dispatch each
      # chain as a single work unit so one worker processes it sequentially.
      tx_groups = rows.group_by { |r| r['transaction_id'] }
                      .sort_by { |_, tx_rows| tx_rows.first['committed_at'] }

      chains = ordered_chains(tx_groups)

      chains.each do |chain|
        deadline = monotonic_now + QUEUE_PUSH_TIMEOUT
        loop do
          begin
            @work_queue.push(chain, true)
            break
          rescue ThreadError
            if monotonic_now >= deadline
              Rails.logger.warn "[PgLwwSync Consumer] Work queue full after #{QUEUE_PUSH_TIMEOUT}s — workers may be hung. Skipping batch."
              return
            end
            sleep 0.1
          end
        end
      end
    end

    # Merges transaction groups that share record_ids into ordered chains.
    # Returns an array of chains, each being an array of [transaction_id, tx_rows]
    # pairs sorted by committed_at. One worker processes each chain sequentially.
    #
    # Example: tx1 INSERTs record A, tx2 UPDATEs record A, tx3 INSERTs record B
    # → chain 1: [tx1, tx2] (must be sequential — same record)
    # → chain 2: [tx3]      (independent — different record)
    def ordered_chains(tx_groups)
      # Build a map: record_id → index of the chain it belongs to
      chains = []        # Array of Arrays of [tx_id, tx_rows]
      record_chain = {}  # record_id → chain index

      tx_groups.each do |transaction_id, tx_rows|
        # Find if any row in this transaction touches a record already in a chain
        record_ids   = tx_rows.map { |r| r['record_id'] }.uniq
        chain_indices = record_ids.filter_map { |rid| record_chain[rid] }.uniq

        if chain_indices.empty?
          # No dependency — start a new chain
          idx = chains.size
          chains << [[transaction_id, tx_rows]]
        elsif chain_indices.size == 1
          # All shared records belong to the same existing chain — append
          idx = chain_indices.first
          chains[idx] << [transaction_id, tx_rows]
        else
          # Records span multiple chains — merge them all into the one with
          # the smallest index. chain_indices is NOT sorted (it reflects
          # whatever order record_ids happen to appear in for this
          # transaction), so we must explicitly exclude idx itself here —
          # dropping the array's first element instead of the min would
          # sometimes leave idx in the merge list, causing chains[idx] to be
          # concatenated with itself and then nulled out mid-merge.
          idx = chain_indices.min
          (chain_indices - [idx]).each do |other_idx|
            chains[idx].concat(chains[other_idx])
            chains[other_idx] = nil  # mark as merged
            # Re-point all records from the merged chain to the surviving one
            chains[idx].each do |_, merged_rows|
              merged_rows.each { |r| record_chain[r['record_id']] = idx }
            end
          end
          chains[idx] << [transaction_id, tx_rows]
        end

        # Register all record_ids in this transaction to the chosen chain
        record_ids.each { |rid| record_chain[rid] = idx }
      end

      chains.compact
    end

    private

    # -----------------------------------------------------------------------
    # Worker lifecycle
    # -----------------------------------------------------------------------

    def spawn_and_register_worker
      t = Thread.new do
        until @shutdown
          chain = @work_queue.pop
          # pop returns nil when the queue is closed (shutdown path)
          break if chain.nil?

          begin
            # Process each transaction in the chain sequentially — ordering
            # guarantees that an INSERT is always delivered before any UPDATE
            # or DELETE for the same record within this worker.
            chain.each do |transaction_id, tx_rows|
              process_transaction_bucket(transaction_id, tx_rows)
            end
          rescue => e
            transaction_id = chain.last&.first
            Rails.logger.error "[PgLwwSync Worker] Unhandled error for tx #{transaction_id}: #{e.class}: #{e.message}"
          ensure
            ActiveRecord::Base.connection_pool.release_connection
          end
        end
      end

      @workers_mutex.synchronize { @workers << t }
      t
    end

    # Called on every main-loop iteration. Removes dead threads and spawns
    # replacements so the pool stays at exactly @pool_size live workers.
    def reap_and_respawn_workers
      @workers_mutex.synchronize do
        dead = @workers.reject(&:alive?)
        dead.each do |t|
          Rails.logger.warn "[PgLwwSync Consumer] Worker thread #{t.object_id} died — respawning."
          @workers.delete(t)
        end
      end

      # Spawn outside the mutex — spawn_and_register_worker re-acquires it internally
      live_count = @workers_mutex.synchronize { @workers.count(&:alive?) }
      (@pool_size - live_count).times { spawn_and_register_worker } if live_count < @pool_size
    end

    # Signals all workers to stop, waits up to GRACEFUL_SHUTDOWN_TIMEOUT for
    # in-flight jobs to complete, then forcibly kills any stragglers.
    def drain_and_shutdown_workers
      Rails.logger.info "[PgLwwSync Consumer] Waiting up to #{GRACEFUL_SHUTDOWN_TIMEOUT}s for workers to drain."

      # Closing the queue unblocks any worker blocked on pop, which then sees
      # a nil item and exits its loop cleanly.
      @work_queue.close if @work_queue.respond_to?(:close)  # SizedQueue#close added in Ruby 2.7

      deadline = monotonic_now + GRACEFUL_SHUTDOWN_TIMEOUT
      @workers_mutex.synchronize { @workers.dup }.each do |t|
        remaining = deadline - monotonic_now
        if remaining > 0
          t.join(remaining)
        end
        if t.alive?
          Rails.logger.warn "[PgLwwSync Consumer] Worker #{t.object_id} did not finish in time — killing."
          t.kill
        end
      end

      Rails.logger.info "[PgLwwSync Consumer] Shutdown complete."
    end

    # -----------------------------------------------------------------------
    # Timing helpers — all use CLOCK_MONOTONIC, never wall-clock Time
    # -----------------------------------------------------------------------

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # -----------------------------------------------------------------------
    # Fetching and processing
    # -----------------------------------------------------------------------

    def fetch_pending_outbox_changesets
      now_mono = monotonic_now

      if @last_horizon_check_mono.nil? || (now_mono - @last_horizon_check_mono) > HORIZON_CACHE_TTL
        @last_horizon_check_mono = now_mono

        begin
          max_active_age = ActiveRecord::Base.connection.select_value(<<~SQL).to_f
            SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - query_start)), 0) AS age
            FROM pg_stat_activity
            WHERE state != 'idle' AND backend_type = 'client backend'
            ORDER BY age DESC LIMIT 1;
          SQL
          @cached_horizon_blocked = (max_active_age > MAX_HORIZON_AGE_SECONDS)
        rescue => e
          Rails.logger.warn "[PgLwwSync Consumer] Failed to query pg_stat_activity: #{e.message}. Defaulting to unblocked."
          @cached_horizon_blocked = false
        end
      end

      sql = if @cached_horizon_blocked
        <<~SQL
          SELECT * FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
          WHERE status = 'pending'
            AND transaction_id IN (
              SELECT transaction_id FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
              WHERE status = 'pending'
              GROUP BY transaction_id
              HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')
            )
          ORDER BY committed_at ASC, id ASC
          LIMIT #{@batch_size};
        SQL
      else
        xmin_horizon = ActiveRecord::Base.connection.select_value(
          "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        ).to_i
        <<~SQL
          SELECT * FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
          WHERE status = 'pending'
            AND transaction_id < #{ActiveRecord::Base.connection.quote(xmin_horizon)}
          ORDER BY committed_at ASC, id ASC
          LIMIT #{@batch_size};
        SQL
      end

      ActiveRecord::Base.connection.select_all(sql).to_a
    end

    def process_transaction_bucket(transaction_id, rows)
      PgLwwSync.remote_nodes.each do |node|
        target_node_id = node[:node_id]

        # Skip if already delivered or permanently failed on this node
        next if rows.all? { |r| %w[delivered failed].include?(parse_processed_nodes(r['processed_nodes'])[target_node_id]) }

        deliver_transaction_bucket(transaction_id, rows, node, target_node_id)
      end
    end

    # Delivers an entire DB-transaction's worth of changesets atomically to one
    # target node. If the whole-transaction attempt fails on a genuine (non-
    # transient) error, the failing row is identified and isolated: the
    # transaction is retried excluding that row, so the rest of the rows in
    # the same original transaction still apply atomically together, and only
    # the single offending row is quarantined — not the entire transaction.
    #
    # Without this isolation, ANY single bad row (e.g. one row from an unrelated
    # table sharing the same DB transaction_id) would roll back and permanently
    # quarantine every other row in the bucket too, even though those rows have
    # nothing wrong with them and would succeed if delivered alone.
    def deliver_transaction_bucket(transaction_id, rows, node, target_node_id, excluded_row_ids: [], remote_conn: nil)
      deliverable_rows = rows.reject { |r| excluded_row_ids.include?(r['id']) }
      return if deliverable_rows.empty?

      # Always reuse an existing connection when bisecting — opening a new
      # connection per recursive call would exhaust the pool (log2(n) levels
      # deep × worker threads × pool_size_per_node).
      execute_delivery = lambda do |conn|
        conn.transaction do
          conn.execute("SET LOCAL lww_sync.applying_replication = 'on';")
          deliverable_rows.each { |row| conn.execute(format_lww_sql_call(row, target_node_id)) }
        end
        @local_db_mutex.synchronize do
          update_local_node_status(deliverable_rows.map { |r| r['id'] }, target_node_id, 'delivered')
        end
      end

      if remote_conn
        execute_delivery.call(remote_conn)
      else
        ReplicationClient.connect_to(node) { |conn| execute_delivery.call(conn) }
      end

    rescue => e
      if transient_error?(e)
        handle_replication_failure(deliverable_rows, target_node_id, e, transient: true)
        return
      end

      if deliverable_rows.size == 1
        handle_replication_failure(deliverable_rows, target_node_id, e, transient: false)
        @local_db_mutex.synchronize do
          quarantine_poison_pill_rows([deliverable_rows.first['id']], target_node_id)
        end
        return
      end

      # Bisect: narrow down to the single bad row across O(log n) retries,
      # reusing the same connection so we don't exhaust the pool.
      # We must get a connection if we don't already have one — but since we
      # just had a non-transient failure the connection may be fine (the error
      # came from the remote DB, not the connection itself). Reuse it.
      # If remote_conn is nil here, the ReplicationClient block above already
      # closed, so we pass nil and let the recursive calls open one each —
      # but since we're at >1 row and bisecting, they share the same pool checkout.
      mid = deliverable_rows.size / 2
      first_half_ids  = deliverable_rows[0...mid].map { |r| r['id'] }
      second_half_ids = deliverable_rows[mid..-1].map { |r| r['id'] }

      ReplicationClient.connect_to(node) do |conn|
        deliver_transaction_bucket(
          transaction_id, rows, node, target_node_id,
          excluded_row_ids: excluded_row_ids + second_half_ids,
          remote_conn: conn
        )
        deliver_transaction_bucket(
          transaction_id, rows, node, target_node_id,
          excluded_row_ids: excluded_row_ids + first_half_ids,
          remote_conn: conn
        )
      end
    end

    # Marks a specific set of outbox rows (by primary key, NOT by transaction_id)
    # as permanently failed for one target node. Scoping by row id is what makes
    # transaction-bucket isolation safe — only the genuinely bad row is
    # quarantined, never its siblings that share the same DB transaction_id.
    def quarantine_poison_pill_rows(row_ids, target_node_id)
      return if row_ids.empty?
      conn = ActiveRecord::Base.connection
      quoted_ids = row_ids.map { |id| conn.quote(id) }.join(", ")
      conn.execute(<<~SQL)
        UPDATE #{PgLwwSync::TARGET_CHANGESET_TABLE}
        SET processed_nodes = processed_nodes || jsonb_build_object(#{conn.quote(target_node_id)}, 'failed')
        WHERE id IN (#{quoted_ids});
      SQL
    end

    # Marks a specific set of outbox rows as delivered/failed for one target
    # node, then promotes each row's status to 'success' independently once
    # every remote node is accounted for. Scoped by row id, not transaction_id,
    # so partial delivery within a bisected transaction bucket only updates the
    # rows that were actually attempted in this call.
    def update_local_node_status(row_ids, target_node_id, state)
      return if row_ids.empty?
      conn = ActiveRecord::Base.connection
      quoted_ids = row_ids.map { |id| conn.quote(id) }.join(", ")

      # Merge delivery state for this target node into processed_nodes.
      conn.execute(<<~SQL)
        UPDATE #{PgLwwSync::TARGET_CHANGESET_TABLE}
        SET processed_nodes = processed_nodes || jsonb_build_object(#{conn.quote(target_node_id)}, #{conn.quote(state)})
        WHERE id IN (#{quoted_ids});
      SQL

      # Promote row status to 'success' once every configured remote node shows
      # 'delivered' or 'failed' in processed_nodes. This is what allows
      # prune_delivered_changesets! to remove rows — without this promotion,
      # row status stays 'pending' forever.
      all_remote_node_ids = PgLwwSync.remote_nodes.map { |n| n[:node_id].to_s }

      if all_remote_node_ids.any?
        all_delivered_conditions = all_remote_node_ids.map do |nid|
          "processed_nodes->>#{conn.quote(nid)} IN ('delivered', 'failed')"
        end.join(" AND
            ")

        conn.execute(<<~SQL)
          UPDATE #{PgLwwSync::TARGET_CHANGESET_TABLE}
          SET status = 'success'
          WHERE id IN (#{quoted_ids})
            AND status = 'pending'
            AND (
              #{all_delivered_conditions}
            );
        SQL
      end
    end

    # Returns true for errors that indicate infrastructure/connectivity problems
    # rather than a problem with the changeset itself. These should be retried
    # rather than quarantined.
    #
    # SQLSTATE classes that indicate transient infrastructure failures:
#   08* — Connection Exception (server unreachable, connection dropped)
#   57* — Operator Intervention (server shutdown, crash, admin cancel)
#   53* — Insufficient Resources (out of memory, disk full — transient in practice)
    def transient_error?(error)
      # Explicit connection-level exceptions that don't carry a SQLSTATE
      return true if error.is_a?(PG::ConnectionBad)
      return true if error.is_a?(PG::UnableToSend)
      return true if error.is_a?(ActiveRecord::ConnectionNotEstablished)

      # For PG errors that do carry a SQLSTATE, inspect the class prefix
      pg_error = error.is_a?(PG::Error) ? error : error.cause
      if pg_error.is_a?(PG::Error) && pg_error.respond_to?(:result) && pg_error.result
        sqlstate = pg_error.result.error_field(PG::Result::PG_DIAG_SQLSTATE).to_s
        return true if sqlstate.start_with?('08', '57', '53')
      end

      false
    end

    def handle_replication_failure(tx_rows, node_id, error_exception, transient:)
      sample_row   = tx_rows.first || {}
      context_data = {
        target_node_id:    node_id,
        failed_schema:     sample_row['table_schema'],
        failed_table:      sample_row['table_name'],
        primary_record_id: sample_row['record_id'],
        action_type:       sample_row['action_type'],
        transaction_id:    sample_row['transaction_id']
      }

      status_line = transient \
        ? "STATUS: Transient failure — will retry when connectivity to #{node_id} is restored."
        : "STATUS: Transaction #{context_data[:transaction_id]} quarantined as poison pill for #{node_id}."

      Rails.logger.error <<~MSG
        === [PgLwwSync Replication Failure] ===
        Target Peer Node: #{node_id}
        Replication Path: #{context_data[:failed_schema]}.#{context_data[:failed_table]} (ID: #{context_data[:primary_record_id]})
        Engine Action:    #{context_data[:action_type]}
        Exception Class:  #{error_exception.class.name}
        Error Details:    #{error_exception.message}
        #{status_line}
        ========================================
      MSG

      if PgLwwSync.on_replication_failure.respond_to?(:call)
        begin
          PgLwwSync.on_replication_failure.call(error_exception, context_data)
        rescue => callback_error
          Rails.logger.error "[PgLwwSync Callback Crash] on_failure block raised: #{callback_error.message}"
        end
      end
    end

    def format_lww_sql_call(row, _target_node_id)
      conn = ActiveRecord::Base.connection
      # record_id is the plain UUID text (tables require a single UUID 'id' PK)
      <<~SQL
        SELECT lww_sync.apply_lww_change(
          #{conn.quote(row['table_schema'])},
          #{conn.quote(row['table_name'])},
          #{conn.quote(row['record_id'])},
          #{conn.quote(row['action_type'])},
          #{conn.quote(row['changed_fields'])}::jsonb,
          #{conn.quote(row['column_timestamps'])}::jsonb,
          #{conn.quote(PgLwwSync.node_id)}
        );
      SQL
    end

    def prune_delivered_changesets!
      cutoff = @prune_retain_days.days.ago
      result = ActiveRecord::Base.connection.execute(<<~SQL)
        DELETE FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
        WHERE status = 'success'
          AND committed_at < #{ActiveRecord::Base.connection.quote(cutoff.iso8601)};
      SQL
      pruned = result.cmd_tuples
      Rails.logger.info "[PgLwwSync Consumer] Pruned #{pruned} delivered changesets older than #{@prune_retain_days} days." if pruned > 0

    rescue => e
      Rails.logger.warn "[PgLwwSync Consumer] Prune failed: #{e.message}"
    end

    def parse_processed_nodes(field)
      return {} if field.nil?
      field.is_a?(Hash) ? field : JSON.parse(field)
    end
  end
end
