# Engine Integrations

## What is an Engine?

Iceberg is a **table format** — a specification for organizing metadata and data files. It
does not execute queries, shuffle data, join tables, or manage compute resources. Engines do.

An engine is a system that actually processes data. It takes a query, distributes work across
a cluster of machines, reads bytes from files, and computes results.

```
User writes:  SELECT * FROM orders WHERE category = 'electronics'
                │
                ▼
┌──────────────────────────────────┐
│         Engine (e.g. Spark)       │
│                                   │
│  1. Parse SQL                     │
│  2. Ask Iceberg: "which files     │
│     do I need to read?"           │  ← Iceberg scan planning
│  3. Iceberg returns: [file_3,     │
│     file_7, file_12]              │
│  4. Engine reads those files      │  ← Via FileIO (S3, GCS, etc.)
│  5. Engine filters/joins/aggs     │
│  6. Engine returns results        │
└──────────────────────────────────┘
```

The core Iceberg API is the contract. Any engine can build an adapter against it:

```
Engine API                    Iceberg Core API
──────────                    ────────────────
Spark ScanBuilder.build()  →  table.newScan().filter(expr).planFiles()
Spark WriteBuilder.build() →  table.newAppend() / table.newRowDelta()
Flink Source.createReader() → table.newScan().planFiles()
Flink SinkWriter.write()   →  TaskWriter.write(record)
MR InputFormat.getSplits() →  table.newScan().planTasks()
```

## Nodes and Clusters

A **node** is a single machine (physical server or cloud VM) with its own CPU, memory, and
disk. A **cluster** is a group of nodes working together as one system.

```
Cluster (3 nodes, 192 cores total)
┌─────────────────────────────────────────────┐
│  ┌─────────┐  ┌─────────┐  ┌─────────┐    │
│  │ Node 1  │  │ Node 2  │  │ Node 3  │    │
│  │ 64 CPU  │  │ 64 CPU  │  │ 64 CPU  │    │
│  │ 256GB   │  │ 256GB   │  │ 256GB   │    │
│  └─────────┘  └─────────┘  └─────────┘    │
│       └────── network ──────────┘           │
└─────────────────────────────────────────────┘
```

Engines like Spark, Flink, and Ray split work into tasks and distribute them across nodes.
One node would compact files sequentially; a 3-node cluster does it 3x faster in parallel.

## Streaming vs Batch

Iceberg itself has no concept of streaming. It only knows about atomic snapshot commits.
What makes something "streaming" is how the engine uses Iceberg's commit API.

```
Batch:
  1. Accumulate a huge dataset (hours/days of data)
  2. Write all files
  3. Commit one snapshot. Done.

Streaming:
  1. Data arrives continuously
  2. Every N seconds/minutes, flush buffered data to files
  3. Commit a snapshot
  4. Go to step 2 (forever)
```

From Iceberg's perspective, both look identical — a series of `AppendFiles.commit()` calls.
Streaming just does it more frequently.

**The small files problem:** Frequent commits create many small files. 60-second commits ×
24 hours = 1,440 snapshots/day, each adding small files. This hurts read performance (more
manifests to scan, more file opens, less efficient compression). Compaction
(`RewriteDataFiles`) merges small files into larger ones to fix this.

**Streaming reads:** Engines can also continuously discover new snapshots and read only new
data. Flink's `IcebergSource` in streaming mode polls for new snapshots and uses
`IncrementalAppendScan` to read only files added since the last snapshot.

## Where Engine Integrations Live

Some engines maintain their Iceberg integration in this repo, others in their own:

| Engine | Integration location | Why |
|--------|---------------------|-----|
| Spark | `spark/` in this repo | Started alongside Iceberg core at Netflix |
| Flink | `flink/` in this repo | Maintained by Iceberg community |
| Kafka Connect | `kafka-connect/` in this repo | Maintained by Iceberg community |
| Hive/MR | `mr/` in this repo | Maintained by Iceberg community |
| Trino | `plugin/trino-iceberg/` in Trino's repo | Trino prefers own release cycle |
| Presto | In Presto's repo | Same reason |
| DuckDB | In DuckDB's repo | Same reason |

Both approaches work because they all depend on the same `iceberg-core` JAR. External
engines sometimes use package-private Iceberg classes by placing adapter code in the
`org.apache.iceberg` package (e.g., Trino's `IcebergManifestUtils` accesses the
package-private `ManifestLists.read()`).

## Spark (`spark/`) — ~296 files, 4 versions (3.4, 3.5, 4.0, 4.1)

The largest and most mature integration. Plugs into Spark's DataSource V2 API.

**Catalog:** `SparkCatalog` wraps any Iceberg `Catalog` as a Spark `TableCatalog`.
`SparkSessionCatalog` lets Iceberg tables coexist with Hive tables. `SparkTable` wraps an
Iceberg `Table` as a Spark `Table`.

**Read path:** `SparkScanBuilder` → `SparkBatchScan` / `SparkMicroBatchStream`. Converts
Spark filter/projection pushdown into Iceberg scan planning. Row-based readers for
Parquet/ORC/Avro, plus vectorized Arrow-based readers for columnar Parquet.

**Write path:** `SparkWriteBuilder` → `SparkWrite`. Converts Spark writes into Iceberg
commits (`AppendFiles`, `OverwriteFiles`, `RowDelta`, `ReplacePartitions`). Supports batch
and micro-batch streaming.

**20 stored procedures** (callable via `CALL catalog.system.procedure_name(...)`):
- Snapshot management: rollback, set-current-snapshot, cherry-pick, fast-forward
- Maintenance: expire-snapshots, remove-orphan-files, rewrite-data-files,
  rewrite-manifests, rewrite-position-delete-files
- Migration: migrate-table, snapshot-table, add-files, register-table
- Statistics: compute-table-stats, compute-partition-stats
- CDC: create-changelog-view

**13 maintenance actions** (distributed Spark implementations):
- Compaction with 3 strategies: bin-pack, sort, z-order
- Manifest rewrite, position delete rewrite, orphan file cleanup, snapshot expiration

**SQL extensions** (ANTLR grammar + analyzer/optimizer/planner):
- `ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD`
- `ALTER TABLE ... CREATE/DROP BRANCH/TAG`
- `ALTER TABLE ... SET WRITE DISTRIBUTION AND ORDERING`
- View DDL

**7 built-in functions:** `years()`, `months()`, `days()`, `hours()`, `bucket()`,
`truncate()`, `iceberg_version()`

**~60 custom metrics** reported to Spark's metrics system.

## Flink (`flink/`) — ~262 files, 3 versions (1.20, 2.0, 2.1)

The primary streaming engine for Iceberg. Uses Flink's FLIP-27 Source and custom sink APIs.

**Catalog:** `FlinkCatalog` wraps an Iceberg `Catalog` as a Flink `Catalog`. Maps Flink
databases to Iceberg namespaces. `FlinkDynamicTableFactory` provides SQL `CREATE TABLE`
integration.

**Source (read) — FLIP-27 architecture:**
- `IcebergSource` → `IcebergEnumerator` (scan planning) → `IcebergSourceReader` (reading)
- Batch mode: plan all files, read once
- Streaming mode: continuously discover new snapshots, read only new files
- Split assignment strategies: simple (round-robin) and locality-aware

**Sink (write):**
- `IcebergSink` → `IcebergStreamWriter` (writes RowData to files) → `IcebergCommitter`
  (commits via `AppendFiles`/`RowDelta`)
- Manifest-based commit protocol: writers produce manifests, committer reads them
- One Iceberg snapshot per Flink checkpoint

**Streaming write flow:**
```
Kafka → IcebergStreamWriter (each subtask)
              │  buffers RowData, writes files
              │  on checkpoint: flush, emit file list
              ▼
         IcebergCommitter (single subtask)
              │  collects file lists from all writers
              │  table.newAppend().appendFile(f1)...commit()
              ▼
         New Iceberg snapshot every checkpoint interval
```

**Range shuffle (~27 files):** Data-distribution-aware shuffling for better file clustering.
Coordinator collects per-subtask statistics, computes global range assignments, broadcasts
back. Map-based (exact) or sketch-based (reservoir sampling).

**Dynamic multi-table sink (~28 files):** Experimental single Flink sink that writes to
multiple Iceberg tables with auto-creation and schema evolution.

**Maintenance subsystem (~43 files):** Flink-native table maintenance running alongside
streaming jobs. Expire snapshots, rewrite data files, delete orphan files. Monitoring source
detects changes → trigger manager → task execution. Distributed locking via JDBC or
ZooKeeper.

## Kafka Connect (`kafka-connect/`) — ~52 files, sink only

Streams records from Kafka topics into Iceberg tables. No source connector.

**Architecture:** Distributed coordinator/worker pattern using a Kafka control topic.

**Commit protocol:**
1. Coordinator (leader task) sends `StartCommit` on control topic
2. Workers flush writes, respond with `DataWritten` + `DataComplete`
3. Coordinator commits to each Iceberg table via `AppendFiles`/`RowDelta`
4. Offsets stored in snapshot summaries for exactly-once semantics

**Key features:** Static and dynamic table routing (regex or field-value), auto table
creation with schema inference, schema evolution, CDC support via transforms (Debezium,
MongoDB Debezium, AWS DMS), branch-aware commits.

**5 SMTs (Single Message Transforms):** `DebeziumTransform`, `MongoDebeziumTransform`,
`DmsTransform`, `KafkaMetadataTransform`, `CopyValue`.

## MapReduce / Hive (`mr/`) — ~8 files, read only

`IcebergInputFormat` — MR v2 InputFormat that runs Iceberg scan planning in `getSplits()`
and reads via Iceberg's data layer. `MapredIcebergInputFormat` — MR v1 adapter. Allows Hive
to query Iceberg tables. No write support.

## Hive Metastore (`hive-metastore/`) — ~17 files, catalog only

`HiveCatalog` — full Iceberg catalog backed by the Hive Metastore Thrift service. Uses HMS
locking (`MetastoreLock`) for commit safety. `HiveClientPool` / `CachedClientPool` manage
pooled Thrift clients. Supports Hive 1/2/3 via dynamic method dispatch. Also supports
Iceberg views.

## Nessie (`nessie/`) — ~6 files, catalog only

`NessieCatalog` — catalog backed by Project Nessie, a git-like versioned metadata store.
Branch/tag-aware table and view management. Transactional multi-table commits through
Nessie's API.

## Delta Lake (`delta-lake/`) — ~5 files, migration tool

`SnapshotDeltaLakeTable` — one-way migration action that replays a Delta transaction log
into Iceberg snapshots. Converts Delta schema → Iceberg schema, replays add/remove file
actions as Iceberg commits. Not a catalog.

## External Engines (not in this repo)

These engines maintain their own Iceberg integrations, all calling the same `iceberg-core`
API:

| Engine | Integration depth |
|--------|------------------|
| Trino | Full: read, write, DDL, procedures, compaction (`optimize`) |
| Presto | Full: read, write, DDL |
| DuckDB | Read-mostly |
| Impala | Read + write |
| Ray | Read-only via PyIceberg (ML-focused, not a SQL engine) |
| PyIceberg | Read + write + basic maintenance (Python-native, single-node) |

## Summary Table

| Module | Type | Read | Write | Catalog | Streaming | Compaction | Files |
|--------|------|------|-------|---------|-----------|------------|-------|
| `spark/` | Compute engine | ✅ | ✅ | ✅ | ✅ micro-batch | ✅ 3 strategies | ~296 |
| `flink/` | Compute engine | ✅ | ✅ | ✅ | ✅ continuous | ✅ built-in | ~262 |
| `kafka-connect/` | Streaming sink | ❌ | ✅ | ❌ | ✅ | ❌ | ~52 |
| `mr/` | Compute engine | ✅ | ❌ | ❌ | ❌ | ❌ | ~8 |
| `hive-metastore/` | Catalog | ❌ | ❌ | ✅ | ❌ | ❌ | ~17 |
| `nessie/` | Catalog | ❌ | ❌ | ✅ | ❌ | ❌ | ~6 |
| `delta-lake/` | Migration | ❌ | ❌ | ❌ | ❌ | ❌ | ~5 |
