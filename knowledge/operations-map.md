# Operations Map

Every Iceberg operation mapped from API interface → core implementation class.

## Class Hierarchy (key base classes)

```
SnapshotProducer                          (core, base for all snapshot-producing ops)
├── FastAppend                            (simple append, no manifest merging)
└── MergingSnapshotProducer               (adds merge logic for data + delete files)
    ├── MergeAppend                       (append with manifest compaction)
    ├── BaseOverwriteFiles                (overwrite by filter or explicit files)
    ├── BaseRowDelta                      (add data + delete files together)
    ├── BaseReplacePartitions             (dynamic partition overwrite)
    ├── StreamingDelete                   (delete files by path or filter)
    └── BaseRewriteFiles                  (replace files with compacted versions)

BaseScan
└── SnapshotScan
    └── DataScan
        └── DataTableScan                (full table scan)

BaseIncrementalScan
├── BaseIncrementalAppendScan            (scan new appends between snapshots)
└── BaseIncrementalChangelogScan         (scan inserts/deletes/updates between snapshots)
```

## Data Write Operations

These produce new snapshots by adding/removing data and delete files.

| API method | Interface | Core class | File | What it does |
|------------|-----------|------------|------|-------------|
| `table.newAppend()` | `AppendFiles` | `MergeAppend` | `core/.../MergeAppend.java` | Append data files, merge small manifests |
| `table.newFastAppend()` | `AppendFiles` | `FastAppend` | `core/.../FastAppend.java` | Append data files, skip manifest merging |
| `table.newOverwrite()` | `OverwriteFiles` | `BaseOverwriteFiles` | `core/.../BaseOverwriteFiles.java` | Delete files by filter + add new files |
| `table.newRowDelta()` | `RowDelta` | `BaseRowDelta` | `core/.../BaseRowDelta.java` | Add data files + delete files (position/equality) |
| `table.newReplacePartitions()` | `ReplacePartitions` | `BaseReplacePartitions` | `core/.../BaseReplacePartitions.java` | Dynamic partition overwrite |
| `table.newDelete()` | `DeleteFiles` | `StreamingDelete` | `core/.../StreamingDelete.java` | Remove data files by path or row filter |
| `table.newRewrite()` | `RewriteFiles` | `BaseRewriteFiles` | `core/.../BaseRewriteFiles.java` | Replace files with compacted versions (same data) |

All write operations go through `SnapshotProducer.commit()` which:
1. Calls `apply()` to build the new snapshot
2. Writes manifest files
3. Writes the manifest list
4. Calls `TableOperations.commit()` to atomically swap metadata

## Read / Scan Operations

These plan which files to read for a query.

| API method | Interface | Core class | File | What it does |
|------------|-----------|------------|------|-------------|
| `table.newScan()` | `TableScan` | `DataTableScan` | `core/.../DataTableScan.java` | Plan files for a filtered/projected query |
| `table.newBatchScan()` | `BatchScan` | via `BatchScanAdapter` | `api/.../BatchScanAdapter.java` | Wraps TableScan with task grouping |
| `table.newIncrementalAppendScan()` | `IncrementalAppendScan` | `BaseIncrementalAppendScan` | `core/.../BaseIncrementalAppendScan.java` | Only new appends between two snapshots |
| `table.newIncrementalChangelogScan()` | `IncrementalChangelogScan` | `BaseIncrementalChangelogScan` | `core/.../BaseIncrementalChangelogScan.java` | Inserts/deletes/updates between snapshots |

Scan planning flow: `TableScan.planFiles()` → `DataScan` → `ManifestGroup` (`core/.../ManifestGroup.java`) which:
1. Reads manifest list to get manifests
2. Filters manifests using partition summaries (`ManifestEvaluator`)
3. Reads matching manifests in parallel
4. Filters data files using column stats (`InclusiveMetricsEvaluator`)
5. Matches delete files to data files by partition and sequence number

## Schema / Metadata Evolution

These are metadata-only changes — no data files are rewritten.

| API method | Interface | Core class | File |
|------------|-----------|------------|------|
| `table.updateSchema()` | `UpdateSchema` | `SchemaUpdate` | `core/.../SchemaUpdate.java` |
| `table.updateSpec()` | `UpdatePartitionSpec` | `BaseUpdatePartitionSpec` | `core/.../BaseUpdatePartitionSpec.java` |
| `table.replaceSortOrder()` | `ReplaceSortOrder` | `BaseReplaceSortOrder` | `core/.../BaseReplaceSortOrder.java` |
| `table.updateProperties()` | `UpdateProperties` | `PropertiesUpdate` | `core/.../PropertiesUpdate.java` |
| `table.updateLocation()` | `UpdateLocation` | `SetLocation` | `core/.../SetLocation.java` |

These commit by calling `TableOperations.commit()` with updated `TableMetadata`.

## Snapshot Lifecycle

| API method | Interface | Core class | File |
|------------|-----------|------------|------|
| `table.expireSnapshots()` | `ExpireSnapshots` | `RemoveSnapshots` | `core/.../RemoveSnapshots.java` |
| `table.manageSnapshots()` | `ManageSnapshots` | `SnapshotManager` | `core/.../SnapshotManager.java` |

`SnapshotManager` handles: rollback, cherry-pick, create/remove branches and tags, fast-forward, retention policies.

## Maintenance

| API method | Interface | Core class | File |
|------------|-----------|------------|------|
| `table.rewriteManifests()` | `RewriteManifests` | `BaseRewriteManifests` | `core/.../BaseRewriteManifests.java` |
| `table.updateStatistics()` | `UpdateStatistics` | `SetStatistics` | `core/.../SetStatistics.java` |
| `table.updatePartitionStatistics()` | `UpdatePartitionStatistics` | `SetPartitionStatistics` | `core/.../SetPartitionStatistics.java` |

## Transactions

| API method | Core class | File |
|------------|------------|------|
| `table.newTransaction()` | `BaseTransaction` | `core/.../BaseTransaction.java` |

`BaseTransaction` wraps a `TransactionTableOperations` that buffers all metadata changes and commits them atomically via `commitTransaction()`.

## Catalog Operations

| API method | Interface | What it does |
|------------|-----------|-------------|
| `catalog.createTable()` | `Catalog` | Creates initial `TableMetadata` JSON + commits via `TableOperations` |
| `catalog.loadTable()` | `Catalog` | Reads metadata JSON, returns `BaseTable` wrapping `TableOperations` |
| `catalog.dropTable()` | `Catalog` | Removes catalog entry, optionally purges files |
| `catalog.renameTable()` | `Catalog` | Atomic pointer swap in catalog backend |

## TableOperations Implementations

`TableOperations` is the SPI that catalogs implement for metadata commit:

| Class | File | Backend |
|-------|------|---------|
| `HadoopTableOperations` | `core/.../hadoop/HadoopTableOperations.java` | Atomic rename on HDFS/local FS |
| `BaseMetastoreTableOperations` | `core/.../BaseMetastoreTableOperations.java` | Base for metastore-backed catalogs |
| `JdbcTableOperations` | `core/.../jdbc/JdbcTableOperations.java` | JDBC database |
| `RESTTableOperations` | `core/.../rest/RESTTableOperations.java` | REST catalog API |

## Distributed Actions (Spark/Flink level)

These are in `api/.../actions/` and implemented by engine-specific modules:

| Action interface | What it does |
|-----------------|-------------|
| `RewriteDataFiles` | Compaction — merge small data files |
| `RewritePositionDeleteFiles` | Compact position delete files |
| `ConvertEqualityDeleteFiles` | Convert equality deletes → position deletes |
| `ExpireSnapshots` (action) | Parallel snapshot expiration |
| `DeleteOrphanFiles` | Clean up unreferenced files |
| `RewriteManifests` (action) | Parallel manifest rewrite |
| `RewriteTablePath` | Relocate table files to new paths |
| `SnapshotTable` / `MigrateTable` | Convert Hive/other tables to Iceberg |
| `ComputeTableStats` | Generate Puffin statistics files |
| `ComputePartitionStats` | Generate partition statistics |
| `RemoveDanglingDeleteFiles` | Remove delete files with no matching data files |
