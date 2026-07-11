# PgLwwSync

`PgLwwSync` is a self-contained, application-level active-active multi-master replication engine designed specifically for **PostgreSQL 13+** and **Ruby on Rails**. 

Unlike native PostgreSQL logical replication—which relies on strict write-ahead log (WAL) decoding and suffers from catastrophic replication halts during write conflicts—`PgLwwSync` leverages an asynchronous application-level transactional outbox pattern combined with a microsecond-accurate **Last-Write-Wins (LWW) Column Clock Matrix** and an automated **Shared-Memory Cluster Routing Layer**. This allows multiple globally distributed database nodes to safely accept write traffic, dynamically survive localized infrastructure outages, and automatically converge to a globally consistent state.

## Key Features
* **Zero-Infrastructure Multi-Master Mesh:** No external message brokers (Kafka, RabbitMQ) or invasive database extensions (`pglogical`, `BDR`) required. It establishes communication lines using standard database connection configurations.
* **Granular Column-Level Conflict Resolution:** Last-Write-Wins (LWW) resolution rules are tracked at the **individual column level**, minimizing data loss and eliminating row-level clobbering during cross-region concurrent updates.
* **Shared-Memory Consensus Routing:** Features a cross-process cluster coordinator that dynamically routes client queries to the optimal healthy database instance, refreshing statuses every 5 seconds via `/dev/shm`.
* **Seamless Request Failover-Retry Middleware:** Transparently intercepts connection dropouts or node crashes inside the web middleware layer (`RequestRouter`), instantly blacklists the dead node, clears shared states, and transparently retries the user's transaction against surviving peers.
* **Multi-Schema Auto-Discovery:** Dynamically discovers, monitors, and links operational application tables while strictly ignoring internal system catalogs, schemas, and tracking definitions.
* **Resilient Connection Fault-Tolerance:** Structural network partitions or extended remote server down-times do not drop transactional history. Records sit safely inside local outbox tables until connection integrity recovers.

---

## Architecture Blueprint

```
                     +---------------------------------------+
                     |         HTTP / Client Request         |
                     +---------------------------------------+
                                         |
                                         v
                     +---------------------------------------+
                     |      PgLwwSync::RequestRouter         |
                     +---------------------------------------+
                                         |
               +-------------------------+-------------------------+
               | (Reads Consensus from /dev/shm/pg_lww_sync.json)  |
               v                                                   v
    [ Primary Node Alive? ]                             [ Primary Node Crashed? ]
               |                                                   |
               v                                                   v
   +-----------------------+                         +---------------------------+
   | Proxies connection to |                         | 1. Deletes SHM state file |
   | designated target db  |                         | 2. Health-checks cluster  |
   +-----------------------+                         | 3. Blacklists dead node   |
               |                                     | 4. Transparently retries  |
               v                                     +---------------------------+
   +-----------------------+                                       |
   | Executes application  |                                       v
   | mutation payload code |                         +---------------------------+
   +-----------------------+                         | Re-routes thread pool to  |
               |                                     | the next healthy peer     |
               v                                     +---------------------------+
+-----------------------------+                                    |
| PL/pgSQL Trigger Log Engine | <----------------------------------+
+-----------------------------+
               |
               v (Appends transaction rows)
+-----------------------------+
| pg_lww_changesets (Outbox)  |
+-----------------------------+
               |
               v (Polled asynchronously by Transactional Buckets)
+-----------------------------+
|    Background Consumer      | ====> Broadcasts payload using Replica Role
+-----------------------------+       to Remote Peer Matrix Clocks
```

---

## Requirements

* **Ruby:** 3.0+
* **Ruby on Rails:** 6.1+
* **Database:** PostgreSQL 13.0+ (Utilizes native JSONB operations, transactional horizons, advisory locking, and `session_replication_role`).
* **Table schema:** every synced table must have a single-column primary key named `id` of type `UUID`. Composite primary keys, non-UUID primary keys (serial/bigint/text), and differently-named primary key columns are not supported — `sync_all_tables!` will skip such tables with a warning rather than attaching the sync trigger. See [Why a UUID Primary Key Is Required](#why-a-uuid-primary-key-is-required) for the reasoning.

---

## Installation & Initial Setup

Add the gem directly to your application's `Gemfile`:

```ruby
gem 'pg_lww_sync'
```

Execute your bundle environment alignment commands:

```bash
bundle install
```

### Setup System Schemas
Generate the cluster networking mapping configurations and system-level database schemas:

```bash
rails generate pg_lww_sync:install
```

Run the generated database migration to create the core synchronization infrastructure (`pg_lww_changesets` outbox table and `apply_lww_change` PL/pgSQL function):

```bash
rails db:migrate
```

### Zero-Configuration Middleware Activation
`PgLwwSync` utilizes a native Rails Railtie engine to automatically bootstrap itself into your application's middleware pipeline on boot.

The engine hooks into the framework initialization lifecycle and automatically mounts `PgLwwSync::RequestRouter` directly before `ActiveRecord::Migration::CheckPending`. This guarantees that cluster consensus topology calculations and destination primary routing are resolved seamlessly before Rails evaluates pending schema migration states or handles active request processing loops.

---

## Core Cluster Topology Configuration

The installer establishes an environment map structure inside `config/pg_lww_sync.yml`. You must define a unique `node_id` for each region and expose database credentials pointing to every peer node across your infrastructure matrix.

```yaml
# config/pg_lww_sync.yml
production:
  # Unique identifier for this node — must not match any remote node_id.
  # Alphanumeric and underscores only, max 64 characters.
  node_id: "us_east_primary"

  # Number of background worker threads delivering changesets to remote nodes.
  # Each worker holds one AR connection. Default: 3.
  # Increase for higher write throughput; ensure pool: >= 1 + worker_threads + 5.
  worker_threads: 3

  # How long to retain delivered outbox changesets before pruning. Default: 7 days.
  # Must be long enough to cover any planned node downtime or maintenance window.
  prune_retain_days: 7

  # How often (seconds) to run the prune job. Default: 3600 (once per hour).
  # Sub-minute values are wasteful given the 7-day retain window.
  prune_interval_seconds: 3600

  # Register all cross-region database peer targets making up your active-active mesh.
  # node_id must be unique across all entries and must not equal the local node_id above.
  remote_nodes:
    - node_id: "eu_west_primary"
      adapter: "postgresql"
      host: "eu-database.yourdomain.internal"
      database: "production_application_db"
      username: "lww_sync_replication"
      password: "<%= ENV['EU_DB_PASSWORD'] %>"
      port: 5432
      pool: 15

    - node_id: "ap_south_primary"
      adapter: "postgresql"
      host: "ap-database.yourdomain.internal"
      database: "production_application_db"
      username: "lww_sync_replication"
      password: "<%= ENV['AP_DB_PASSWORD'] %>"
      port: 5432
      pool: 15
```

---

## Understanding the Engine Architecture

### 1. The Consensus Engine (`PgLwwSync::Consensus`)
To protect against race conditions across distributed multi-process environments (like multiple Puma or Unicorn workers running on the same server), the consensus system implements a hybrid **Fast-Path / Lock-Protected** shared-memory strategy:
* **Fast Path:** Workers check an in-memory JSON state representation at `/dev/shm/pg_lww_sync.json`. If it matches the current 5-second interval tick, the request skips the network and routes instantly to the cached primary.
* **Safe Path:** If the cache expires, workers trigger a standard POSIX file-lock (`flock`). The single worker holding the lock spins up background check threads to verify cluster health and confirm `pg_is_in_recovery()` states, saving the newest primary to shared memory for everyone else to consume.

### 2. The Failover Routing Loop (`RequestRouter`)
The custom HTTP request processor (`PgLwwSync::RequestRouter`) completely automates multi-node database fault-tolerance:
* **Dynamic Target Routing:** For every incoming request, it query-checks the consensus layer and wraps the active thread pool context around the chosen target instance using a custom-named dynamic pool connection handler (`pg_sync_pool_<node_id>`).
* **Automated Node Outage Interception:** If a database node drops mid-execution or crashes before a transaction commits, the middleware intercepts the lower-level connection error (`PG::ConnectionBad`, `ActiveRecord::ConnectionNotEstablished`, or connection-dropped `StatementInvalid`).
* **Instant Blacklist & Recovery:** The middleware immediately wipes the shared memory status file `/dev/shm/pg_lww_sync.json` to flag a failure event to the machine. It blacklists the failed node ID for the remainder of that specific request lifecycle, re-runs cluster calculations, hooks into a surviving healthy database peer, and **re-tries the transaction transparently** without throwing an error page back to your users.

### 3. Composite Write Key LWW Matrix
When records are inserted or updated, the trigger writes a **composite write key** into a `__lww_ts` JSONB column stored directly on each application row. The write key embeds both the timestamp and the origin node in a single TEXT value:

```
"001704067200.123456_node_b"
 └──────────────────────┘ └─────┘
  zero-padded epoch (µs)   node_id
```

A single `TEXT >` comparison resolves both LWW ordering and tiebreaking simultaneously:
* **Newer write wins:** a larger epoch prefix means the write arrived later, so it takes priority over the local value.
* **Transitive tiebreak:** when two writes share the exact same microsecond, the node_id suffix determines the winner lexicographically. Because the tiebreak compares the two actual writers against each other (not against the evaluating node's own identity), every node in the cluster reaches the identical decision — even when the value has already passed through an intermediate node.

Write keys are stored per-column in `__lww_ts`, so concurrent updates to different columns on the same row are resolved independently — Node A updating `name` and Node B updating `email` at the same time both win their respective columns without either clobbering the other.

Because `__lww_ts` lives on the row itself, timing data and application data are always atomic (same transaction, same WAL entry) and travel together in `pg_dump` snapshots — no separate metadata table to manage or synchronise.

### 4. Why a UUID Primary Key Is Required

Every table with the sync trigger attached must have a single-column primary key named `id` of type `UUID`. `sync_all_tables!` checks this at trigger-install time and silently skips (with a warning) any table that doesn't conform — no trigger is attached, and writes to that table are never captured in the outbox.

This requirement exists for two reasons:

* **Collision-free inserts across nodes.** A UUID generated independently on any node has a cryptographically negligible chance of colliding with a UUID generated on any other node. This is what makes INSERTs unconditionally safe (see [Terminal Delete Semantics](#5-terminal-delete-semantics) below) — two nodes can never accidentally generate the same `id` for two different logical records the way they could with auto-incrementing integers.
* **Performance.** A fixed-width, single-column UUID key keeps every `record_id` comparison and `__lww_ts` lookup in `apply_lww_change` to a single, constant-cost operation. An earlier version of this gem supported arbitrary composite primary keys, which required a system-catalog scan (`pg_index`/`pg_attribute`) on every trigger fire plus variable-length text comparisons per lookup — measurably slower at high write volumes. The single UUID requirement eliminates all of that.

If you have existing tables with integer or composite primary keys that need to participate in active-active sync, add a `UUID` column named `id` (with `gen_random_uuid()` as the default) and either drop the existing primary key in favor of it, or keep your existing key as a unique index alongside the new UUID primary key for backward compatibility with existing foreign keys and application code.

### 5. Conflict Resolution for Unique Indexes

A UUID primary key prevents primary-key collisions, but it does nothing to prevent two nodes from independently inserting two different rows that violate a **unique index on a non-PK column** — for example, two nodes each creating a user with `email = "same@example.com"` during a network partition. This is a real scenario in active-active replication and `pg_lww_sync` resolves it automatically rather than treating it as a hard failure.

**How it works:**

When `apply_lww_change` attempts an INSERT and PostgreSQL raises `unique_violation`, the function does not give up or quarantine the changeset. Instead it:

1. **Finds the conflicting row.** It walks every unique index on the table (excluding the primary key) and, using the incoming record's values for that index's columns, looks for an existing local row that matches. NULL values in unique index columns are matched with `IS NULL` rather than `= NULL`, consistent with how PostgreSQL itself treats NULLs in unique indexes.
2. **Applies First-Write-Wins (FWW).** It reads the `__lww_ts` JSONB column on both the incoming record and the local conflicting row, takes the MAX write key from each, and compares them as plain TEXT. The record with the **smaller** key (earlier timestamp + lower node_id) survives. This matches single-node PostgreSQL behaviour: if both writes had arrived sequentially, the first-committed write would have succeeded and the unique constraint would have rejected the second.
3. **Tiebreak is embedded.** The node_id suffix in the write key provides a deterministic, transitive tiebreak with no extra lookup — the same single TEXT comparison handles both ordering and tiebreaking.
   * **Incoming has the smaller key (first writer)** → the local conflicting row is deleted (its `__lww_ts` is removed with it), conflicts are re-scanned, and the INSERT is retried.
   * **Local has the smaller key (first writer)** → the incoming INSERT is discarded.

**Note on missing write keys:** rows inserted by migrations, seeds, or direct DB writes that bypass the trigger have an empty `__lww_ts` (`{}`). These are treated as having been written at `''` (empty string) — they always lose to any row that went through the normal write path, since any composite key is lexicographically greater than an empty string.

**Example — concurrent user creation during a partition:**

```
Node A (NA, offline from EU): INSERT users (id: uuid-A, email: "same@example.com") at T=100
Node B (EU, offline from NA): INSERT users (id: uuid-B, email: "same@example.com") at T=105
```

When connectivity restores and Node A's changeset reaches Node B:
* Node B already has `uuid-B` with `email = "same@example.com"` (T=105).
* The incoming INSERT for `uuid-A` (T=100) violates the unique index on `email`.
* `apply_lww_change` finds the conflicting row (`uuid-B`), compares timestamps: incoming (T=100) is **earlier** than local (T=105).
* **Incoming wins (FWW).** Node B deletes `uuid-B` and inserts `uuid-A`.

When Node B's changeset reaches Node A:
* Node A has `uuid-A` with `email = "same@example.com"` (T=100).
* The incoming INSERT for `uuid-B` (T=105) violates the unique index on `email`.
* Incoming (T=105) is **later** than local (T=100). **Local wins (FWW).** The incoming INSERT for `uuid-B` is discarded. Node A keeps `uuid-A`.

Both nodes converge on `uuid-A` as the surviving record — the earliest writer wins on every node, deterministically, with no manual intervention.

**Limitations:**

* This resolves **value-level** conflicts (two different unique constraint violations resolved independently per index), not **business-level** conflicts. If your application has logic that depends on which specific row survives (e.g. "the first user to sign up with this email gets a welcome bonus"), the FWW semantic is actually the natural fit — the first writer does win. But the application should still handle the case where a write it thought succeeded is later found to have lost, since the "first writer" is determined by wall-clock timestamp and not by which request the application processed first.
* Multiple unique indexes violated simultaneously are resolved one at a time. If the incoming row violates index A against local row X, and index B against local row Y, and the incoming row is the earliest writer in both cases, X and Y are both deleted and the incoming row is inserted. If the incoming row is the latest writer on any one index, it loses immediately and the remaining indexes are not evaluated.

### 6. Terminal Delete Semantics
Deletes are treated as a terminal operation with respect to **UPDATE** changesets. Once a record is deleted, a stale UPDATE arriving from a node that hasn't yet received the delete — carrying a newer timestamp due to clock drift — is silently discarded. This prevents silent data resurrection across the cluster.

**How it works:**
* When a row is deleted, the trigger writes an empty `column_timestamps` payload (`{}`) to the outbox. No timestamp is tracked for the deletion itself.
* When a peer node consumes the delete changeset, `apply_lww_change` executes the `DELETE` unconditionally. The `__lww_ts` column is deleted with the row — no explicit cleanup needed.
* A stale UPDATE arriving from a lagging node matches 0 rows and is a silent no-op — no explicit guard needed.
* A recycle bin restore simply re-INSERTs the record. INSERTs are unconditionally safe.

### 7. Background Transaction Buckets (Consumer Execution) & Atomicity Preservation
The asynchronous replication daemon processes outbound changesets by parsing the ledger outbox and grouping rows cleanly by their native database `transaction_id`.
* **Parallel Transaction Distribution:** Unique transaction IDs are assigned across worker threads using a deterministic modulo strategy (`index % pool_size`), routing rows into separate thread-safe memory channels (`@queues`).
* **Atomicity Maintenance:** Whole transactions are wrapped within an isolated database block (`remote_conn.transaction`) on the destination peer. This guarantees that multi-row mutations are evaluated as a single atomic element, preventing partial data leaks and protecting relational database foreign-key constraints on remote targets.

---

## Advanced Self-Healing Mechanics

### 1. Transaction Horizon Deadlock Protection
In its default high-performance path, the background consumer isolates committed data boundaries using a database transaction snapshot low-water mark strategy (`transaction_id < txid_snapshot_xmin(txid_current_snapshot())`) to ensure it only reads safely committed rows.

#### The Pitfall
Because PostgreSQL relies on Multi-Version Concurrency Control (MVCC), the low-water mark (`xmin`) of a transaction snapshot is bound to the *oldest currently active transaction across the entire database server*. If an unrelated workflow—such as a long-running data migration, a heavy analytical report, or an unclosed manual SQL console session—stalls on the database node, the global `xmin` horizon freezes. This paralyzes the standard replication stream, causing outbox ledger records to accumulate and replication lag to spike linearly.

#### The Circuit-Breaker Cache Strategy
To protect against replication paralysis, the consumer features an automated **Decoupled State Polling Backoff** engine. Every 10 seconds, the consumer worker audits `pg_stat_activity` to inspect the age of active database blocks. If a frozen transaction horizon exceeds your configured ceiling limit (`max_horizon_age_seconds: 60`), the consumer trips a local circuit breaker and dynamically pivots its lookup strategy to a **Sliding Time-Window Filter**:

```sql
GROUP BY transaction_id
HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')
```

This bypasses the frozen MVCC snapshot boundary entirely, allowing active replication to flow around the stalled transaction without skipping a beat.

### 2. Guarding Fallback Transaction Split Boundaries
When the background replication daemon shifts into the sliding time-window filter fallback mode, a multi-row transaction could have individual changesets written across a microsecond boundary that straddles the cutoff edge. 

`PgLwwSync` handles this using a **Windowed Subquery Aggregation Layer**. During fallback, the consumer groups rows through an explicit `HAVING MAX(committed_at) < (clock_timestamp() - interval '5 seconds')` filter. By checking that the *latest* written component of an entire transaction group is safely older than the safety cutoff interval, it guarantees that a transaction ID is never split across two separate replication loops.

### 3. Poison Pill Isolation vs Transient Failure Retry
When delivery to a peer node fails, the consumer distinguishes between two fundamentally different situations and handles each correctly rather than treating every failure the same way:

* **Transient infrastructure failures** — connection refused, connection dropped mid-transaction, server restart, out of resources. These are classified by `transient_error?` using SQLSTATE class prefixes (`08*` Connection Exception, `57*` Operator Intervention, `53*` Insufficient Resources) plus explicit checks for `PG::ConnectionBad` and `ActiveRecord::ConnectionNotEstablished`. The row-level `status` is left as `'pending'` and the changeset is automatically retried on the next poll cycle once the node recovers. Nothing is quarantined — this is exactly the scenario you want self-healing for (see [Regional Split-Brain](#regional-split-brain-and-network-partitions) below).
* **Genuine schema or data conflicts** — undefined column, undefined table, a constraint violation that `apply_lww_change`'s unique-index resolution couldn't apply automatically (see [Conflict Resolution for Unique Indexes](#5-conflict-resolution-for-unique-indexes)), or malformed SQL. These will fail identically on every retry, so retrying forever would permanently block all other changesets behind it in the queue. The daemon catches the exception and writes `'failed'` into `processed_nodes` for that specific node only — it does **not** set the row-level `status` to `'failed'`. The changeset still gets delivered to every other healthy node normally.

**Why the row itself is never marked `'failed'`:** a schema problem on one node doesn't mean the changeset is bad — it means that *one node's* schema is out of sync with the rest of the cluster. Marking the whole row `'failed'` would incorrectly block delivery to nodes that have no problem at all. Instead:
* `rails pg_lww_sync:status` reports per-node failure counts pulled directly from `processed_nodes`, so you can see exactly which node has fallen out of schema sync and needs operator attention.
* Once that node's schema is fixed, there is currently no automatic re-delivery of quarantined changesets to it — this requires either replaying the affected rows manually or restoring that node from a fresh snapshot of a healthy peer (see [Adding a New Node to an Existing Cluster](#adding-a-new-node-to-an-existing-cluster), which applies equally to re-adding a repaired node).

---

## Recipes

### Recycle Bin Pattern

Recycle bin restores work without any special API calls. A genuine INSERT cannot know the PK of a deleted record unless it is an intentional restore, so `pg_lww_sync` applies INSERTs unconditionally. A temporarily-offline node is fully safe: when it comes back up it processes the INSERT from the outbox and applies the row.

#### Example

```ruby
class OrdersController < ApplicationController
  def restore
    deleted = DeletedOrder.find(params[:id])

    ActiveRecord::Base.transaction do
      # Re-insert the record — the trigger fires and writes an INSERT changeset
      # to the outbox. Every peer will apply the row
      # when it processes this changeset, including nodes that were offline.
      Order.create!(deleted.attributes_for_restore)

      # Remove from the soft-delete store
      deleted.destroy!
    end
  end
end
```

#### What happens across the cluster

```
Node A (online):  recycle bin INSERT → trigger fires → outbox changeset written
                  apply_lww_change: action=INSERT → apply row ✓

Node B (online):  receives INSERT changeset from outbox
                  apply_lww_change: action=INSERT → apply row ✓

Node C (offline): comes back online → pulls INSERT changeset from outbox
                  apply_lww_change: action=INSERT → apply row ✓
```

No special handling required. The INSERT changeset carries the restore intent durably through the outbox.

---

## Workers, Console, and Distributed Locking

### The Problem

The `RequestRouter` middleware routes HTTP requests to the consensus-elected primary — but workers (Sidekiq, Solid Queue, etc.) and Rails console sessions bypass Rack entirely. They connect via `ActiveRecord::Base.connection`, which goes to whatever database is configured in `database.yml` — not the elected primary.

For most worker writes this is fine. Local writes are captured by the trigger, written to the outbox, and replicated to all peers via LWW conflict resolution. A worker on Node B writing to Node B's local database is correct and expected.

**The exception is distributed coordination.** If you use PostgreSQL advisory locks (`pg_advisory_lock`) or table-level locking to ensure mutual exclusion across the cluster — for example, to prevent two nodes from processing the same job simultaneously — you need all participants to acquire the lock from the **same database server**. A lock on Node A and a lock on Node B are completely independent; both callers proceed, defeating the purpose.

### `PgLwwSync.with_primary`

`PgLwwSync.with_primary` routes all ActiveRecord calls within the block to the consensus-elected primary, using the same connection pool and failover logic as the middleware. This works from anywhere — workers, console, rake tasks, or initializers.

```ruby
PgLwwSync.with_primary do
  # All AR calls here go to the primary
  ActiveRecord::Base.connection.execute("SELECT pg_advisory_lock(42)")
  MyModel.where(...).update_all(status: "processing")
end
```

When sync is disabled (no `node_id` or no `remote_nodes` configured), `with_primary` yields directly without any connection switching — the local database is the only one, so no routing is needed.

Raises `PgLwwSync::NoPrimaryError` if no healthy primary can be elected at call time. Callers should rescue this and decide whether to retry, defer, or skip:

```ruby
begin
  PgLwwSync.with_primary do
    ActiveRecord::Base.connection.execute("SELECT pg_advisory_lock(#{job_type_id})")
    process_job
  end
rescue PgLwwSync::NoPrimaryError => e
  Rails.logger.warn "[MyWorker] Could not reach primary: #{e.message} — retrying in 5s"
  raise Sidekiq::JobRetry::Skip # or however your worker handles retries
end
```

### Advisory Lock Pattern for Workers

```ruby
class MyExclusiveJob
  include Sidekiq::Job

  LOCK_KEY = 1234567 # any stable integer unique to this job type

  def perform
    PgLwwSync.with_primary do
      acquired = ActiveRecord::Base.connection
        .select_value("SELECT pg_try_advisory_lock(#{LOCK_KEY})")

      unless acquired
        Rails.logger.info "Another node is already running #{self.class.name} — skipping"
        return
      end

      begin
        do_work
      ensure
        ActiveRecord::Base.connection.execute("SELECT pg_advisory_unlock(#{LOCK_KEY})")
      end
    end
  end
end
```

`pg_try_advisory_lock` returns immediately with `true` if the lock was acquired or `false` if another session already holds it — no blocking. Because all nodes call `with_primary`, the lock lives on one server and mutual exclusion is guaranteed cluster-wide.

### Rails Console

```ruby
# Connect to the primary for any writes that need global coordination
PgLwwSync.with_primary { User.find(id).update!(role: "admin") }

# Local writes are fine without with_primary — they replicate via outbox
User.find(id).touch
```

## Adding a New Node to an Existing Cluster


Adding a node to a live cluster is a multi-step process. Doing it out of order risks data loss, split-brain divergence, or the new node receiving changesets it cannot yet apply.

The core constraint is: **the new node must have a consistent copy of the data before it starts receiving changesets, and it must not generate outbox changesets of its own until it is ready to be a peer.**

---

### Step 1 — Provision and migrate the new node

Set up the new PostgreSQL instance and run all Rails migrations against it:

```bash
DATABASE_URL=postgres://new-node/db rails db:migrate
```

Do **not** add the new node to `pg_lww_sync.yml` yet. The node should be completely invisible to the cluster at this point.

---

### Step 2 — Take a consistent base snapshot from an existing node

Use `pg_dump` to take a consistent snapshot of the data from any existing live node. This snapshot will be the authoritative starting state for the new node.

```bash
pg_dump \
  --no-owner --no-acl \
  --exclude-table=pg_lww_sync.pg_lww_changesets \
  --format=custom \
  postgres://existing-node/db \
  > snapshot.dump
```

Only the outbox table (`pg_lww_changesets`) is excluded — it contains in-flight replication state that is meaningless to a new node. The `__lww_ts` column is part of every application row and travels with the snapshot automatically — no special handling required. Pre-snapshot changesets are discarded by the LWW write key comparison because the snapshot's `__lww_ts` values are already at least as recent. The PL/pgSQL functions and triggers are reinstalled fresh in Step 3 with the correct `node_id`.

Restore the snapshot to the new node:

```bash
pg_restore   --no-owner --no-acl   --dbname=postgres://new-node/db   snapshot.dump
```

---

### Step 3 — Initialise pg_lww_sync on the new node

With the new node's `node_id` set in its own `pg_lww_sync.yml`, run the installer and realign triggers:

```bash
# Against the new node
DATABASE_URL=postgres://new-node/db rails generate pg_lww_sync:install
DATABASE_URL=postgres://new-node/db rails db:migrate
DATABASE_URL=postgres://new-node/db rails pg_lww_sync:realign
```

This creates the `pg_lww_sync` schema, installs the PL/pgSQL functions with the correct `node_id` baked in, and attaches the sync triggers to all tables.

---

### Step 4 — No outbox backfill needed

No special action is required here. When the existing nodes' consumers start delivering their outbox to the new node, `apply_lww_change` handles everything correctly:

- **Changesets that predate the snapshot** — the LWW write key comparison sees the incoming key is smaller than or equal to what is already in `__lww_ts` on the row (the data came from the snapshot), so the write is discarded. Idempotent and harmless.
- **Changesets written after the snapshot** — these are genuinely new data the new node does not have yet. They apply normally.

The new node self-heals through the standard replication flow with no operator intervention.

---

### Step 5 — Add the new node to all existing nodes' configuration

Update `config/pg_lww_sync.yml` on every existing node to include the new node in `remote_nodes`:

```yaml
remote_nodes:
  - node_id: "new_node_id"
    host: "new-node.yourdomain.internal"
    database: "production_application_db"
    username: "lww_sync_replication"
    password: "<%= ENV['NEW_NODE_DB_PASSWORD'] %>"
    port: 5432
    pool: 15
```

Add all existing nodes to the new node's `remote_nodes` in the same way.

---

### Step 6 — Restart all nodes

Restart the Rails application on every node (including the new one) so that:

- The new `remote_nodes` configuration is loaded
- `initialize_database_functions!` runs on boot, embedding the correct `node_id` into each node's PL/pgSQL functions via `CREATE OR REPLACE`
- The consumer supervisor starts on each node and begins replicating to the new peer

```bash
# Rolling restart on each node — order does not matter
bundle exec puma --restart
```

---

### Step 7 — Verify replication is flowing

Monitor the outbox on each existing node. New changesets should appear in `processed_nodes` with the new node's ID as they are delivered:

```bash
# Check outbox health on any node
rails pg_lww_sync:status
```

You can also query directly:

```sql
SELECT
  status,
  processed_nodes,
  committed_at
FROM pg_lww_sync.pg_lww_changesets
ORDER BY committed_at DESC
LIMIT 20;
```

New rows should show `processed_nodes` containing an entry for every node including the new one once delivered. If the new node's entry is absent or stuck on `pending`, check `Rails.logger` for consumer errors on the relevant node.

---

### Summary checklist

| Step | Action | Node(s) |
|------|--------|---------|
| 1 | Provision and migrate new node | New node |
| 2 | Snapshot existing data, restore to new node | Existing → New |
| 3 | Run `pg_lww_sync:install` and `realign` | New node |
| 4 | Mark existing outbox history as delivered to new node | All existing nodes |
| 5 | Add new node to `remote_nodes` in config | All nodes |
| 6 | Restart application on all nodes | All nodes |
| 7 | Verify replication is flowing | All nodes |

---

## Production Maintenance & Rake Tasks

### Replaying Failed Quarantined Transactions
Once you address structural schema alignment differences on a lagging node (by running the necessary database migration), you can instruct `PgLwwSync` to flush and retry the quarantined outbox rows back into active synchronization pipeline processing by running:

```bash
bundle exec rake pg_lww_sync:replay_failed
```

### Manual Realignment Triggering
If you perform extensive manual DDL operations, deploy massive schema changes outside normal ActiveRecord migrations, or introduce new structural tables, you can force a cold recompile and realign tracking vectors across all schemas by running:

```bash
bundle exec rake pg_lww_sync:realign
```

---

## Troubleshooting & Custom Error Reporting Callback

`PgLwwSync` does not force an opinionated error tracking dependency onto your application, nor does it ask developers to manually audit database log tables for replication faults. Structural and data-type synchronization anomalies bubble up into Ruby naturally from the target database driver.

### 1. Core Structural Crash Logging
If an outbound replication batch fails due to an out-of-sync regional schema state (e.g., a rolling deployment lag where a newly added column hasn't been migrated to a remote peer node yet), the background daemon formats a prominent log alert inside your standard application stream (`Rails.logger.error`):

```
=== [PgLwwSync Poison Pill Quarantined] ===
Target Peer Node: eu_west_primary
Replication Path: public.users (ID: 92831)
Engine Action:    UPDATE
Exception Class:  ActiveRecord::StatementInvalid
Error Details:    PG::UndefinedColumn: ERROR: column "discount_tier" does not exist
STATUS:           Transaction 5122134 marked as 'failed'. Stream bypassing safely.
===========================================
```

### 2. Configuring Custom Error Tracker Callbacks
You can intercept replication errors and route them directly to your preferred crash monitoring stack (Airbrake, Errbit, Rollbar, Appsignal, Sentry, or internal Slack webhooks) by registering a custom configuration block.

Create an initializer file at `config/initializers/pg_lww_sync.rb` and define an `on_failure` block handler:

```ruby
# config/initializers/pg_lww_sync.rb
PgLwwSync.on_failure do |exception, context|
  # context is a hash containing:
  # :target_node_id, :failed_schema, :failed_table, :primary_record_id, :action_type, :transaction_id

  if defined?(Airbrake)
    Airbrake.notify(exception, parameters: context)
  elsif defined?(Rollbar)
    Rollbar.error(exception, context)
  end
  
  # Example: Pipe alerts directly into an internal developer Slack channel
  SlackNotifier.ping(
    "⚠️ *Replication Poison Pill Quarantined on #{context[:target_node_id]}*: " \
    "Failed to sync table `#{context[:failed_table]}` for Record ID: #{context[:primary_record_id]}. " \
    "Transaction has been bypassed safely."
  )
end
```

### 3. Common Structural Synchronization Anomalies
When configuring alert routing logic or troubleshooting issues caught by your callback block, keep an eye out for these typical multi-region infrastructure events:
* **`PG::UndefinedColumn`:** Occurs during staggered production releases. Node A streams a changeset containing a newly migrated column attribute to Node B before Node B has finished executing its corresponding `rails db:migrate` sequence.
* **`PG::DatatypeMismatch`:** Triggered if database column type updates diverge or are misaligned between regional clusters (e.g., attempting to stream alphanumeric characters into a field that a peer node still enforces as an integer scalar).
* **`PG::StringDataRightTruncation`:** Occurs when character threshold boundaries differ across target nodes (e.g., Node A saves a 120-character string into a field defined as `text`, but Node B rejects it because its local table configuration still binds that field to a restricted `varchar(50)` limit constraint).

**Resolution Procedure:** Once you receive a schema mismatch alert from your tracker, simply bring the target node's database catalog into alignment by running the missing migration setup steps. Once aligned, run `rake pg_lww_sync:replay_failed` to clear out the quarantine queue.

---

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
```