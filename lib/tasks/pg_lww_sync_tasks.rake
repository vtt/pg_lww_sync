namespace :pg_lww_sync do
  desc "Refresh local_node_id() and reattach sync triggers to all tables."
  task realign: :environment do
    if PgLwwSync.sync_enabled?
      puts "Refreshing local_node_id() function..."
      PgLwwSync.initialize_database_functions!
      puts "Scanning tables and attaching triggers..."
    else
      puts "PgLwwSync: Sync disabled (node_id not set) — dropping triggers. Outbox left intact."
    end

    PgLwwSync.sync_all_tables!

    puts "PgLwwSync realignment complete."
  end

  desc "Show counts of pending / success changesets, and nodes with failed deliveries."
  task status: :environment do
    rows = ActiveRecord::Base.connection.select_all(<<~SQL).to_a
      SELECT status, COUNT(*) AS count
      FROM #{PgLwwSync::TARGET_CHANGESET_TABLE}
      GROUP BY status
      ORDER BY status;
    SQL

    puts "\nPgLwwSync outbox status:"
    rows.each { |r| puts "  #{r['status'].ljust(10)} #{r['count']}" }

    # Show per-node failure counts so operators can identify broken nodes
    failed_nodes = ActiveRecord::Base.connection.select_all(<<~SQL).to_a
      SELECT node_entry.key AS node_id, COUNT(*) AS failed_count
      FROM #{PgLwwSync::TARGET_CHANGESET_TABLE},
           jsonb_each_text(processed_nodes) AS node_entry(key, value)
      WHERE node_entry.value = 'failed'
      GROUP BY node_entry.key
      ORDER BY node_entry.key;
    SQL

    if failed_nodes.any?
      puts "\nNodes with schema/constraint failures (require operator attention):"
      failed_nodes.each { |r| puts "  #{r['node_id'].ljust(30)} #{r['failed_count']} changeset(s) failed" }
    end
    puts
  end
end
