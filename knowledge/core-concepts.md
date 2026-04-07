# Core Concepts

This file explains the four foundational concepts in Iceberg's design: partition specs,
partitions, manifests, and snapshots. Each builds on the previous one.

## 1. Partition Spec

A partition spec is a **recipe** that tells Iceberg how to group rows into files when writing.

It's defined as a list of partition fields, where each field says: "take this column, apply
this transform, and use the result to group rows."

### Example

Given a table with columns `id` (long), `ts` (timestamp), and `category` (string):

```java
PartitionSpec spec = PartitionSpec.builderFor(schema)
    .day("ts")              // extract day from timestamp
    .identity("category")   // use category value as-is
    .bucket("id", 16)       // hash id into 16 buckets
    .build();
```

This spec has 3 partition fields:

| Partition field | Source column | Transform | What it does |
|----------------|--------------|-----------|-------------|
| `ts_day` | `ts` | `day` | Extracts the date as days since 1970-01-01 |
| `category` | `category` | `identity` | Passes the value through unchanged |
| `id_bucket` | `id` | `bucket[16]` | Computes `murmur3_hash(id) % 16` |

A transform is simply a function: `transform(column_value) → partition_value`. The available
transforms are:

| Transform | What it does | Example |
|-----------|-------------|---------|
| `identity` | Value as-is | `"electronics"` → `"electronics"` |
| `year` | Years since 1970 | `2024-03-15` → `54` |
| `month` | Months since 1970-01 | `2024-03-15` → `650` |
| `day` | Days since 1970-01-01 | `2024-03-15` → `19797` |
| `hour` | Hours since epoch | `2024-03-15T10:00` → `475138` |
| `bucket[N]` | Hash mod N | `42` with N=16 → `11` |
| `truncate[W]` | Truncate to width W | `"iceberg"` with W=3 → `"ice"` |
| `void` | Always null | Used to "drop" a partition field |

Code: `PartitionSpec` is in `api/.../PartitionSpec.java`. Each `PartitionField` (in
`api/.../PartitionField.java`) stores a `sourceId`, `fieldId`, `name`, and `Transform`.
The `Transform` interface is in `api/.../transforms/Transform.java` with a single key
method: `O apply(I value)`.

## 2. Partitions

A partition (or partition tuple) is the **result** of applying the partition spec to a row.

### Example

Given the spec above and this row:

```
id = 42,  ts = 2024-03-15T10:30:00Z,  category = "electronics"
```

Iceberg applies each transform:

```
day("2024-03-15T10:30:00Z")  →  19797
identity("electronics")      →  "electronics"
bucket[16](42)               →  11
```

The partition tuple is `(19797, "electronics", 11)`.

**All rows in a single data file must have the same partition tuple.** This is what
"partitioned" means — when writing, rows are grouped by their partition values so that each
file contains only rows with one specific tuple.

So if you insert 1 million rows spanning March 13-16 across 3 categories:

```
file_01.parquet  →  partition: (19795, "electronics", 3)    ← all March 13, electronics, bucket 3
file_02.parquet  →  partition: (19795, "clothing", 7)       ← all March 13, clothing, bucket 7
file_03.parquet  →  partition: (19796, "electronics", 3)    ← all March 14, electronics, bucket 3
...and so on
```

This physical grouping is what makes skipping possible during reads.

### Hidden partitioning

Users never see partition values. Unlike Hive, where you must add a `event_date` column and
filter on it explicitly, Iceberg derives partition values automatically from data columns.
A query like `WHERE ts > '2024-03-15'` is automatically converted to a partition predicate
`ts_day >= 19797` using inclusive projection. Users write queries against data columns;
Iceberg handles the rest.

Code: `PartitionKey` (`api/.../PartitionKey.java`) computes partition tuples. Its
`partition(StructLike row)` method applies each transform to the row. Predicate conversion
happens via `Projections.inclusive()` in `api/.../expressions/Projections.java`.

## 3. Manifests and Manifest Lists

### Manifest

A manifest is an **Avro file that lists data files** along with metadata about each file.

For each data file, a manifest stores:
- File path (e.g., `s3://bucket/data/file_01.parquet`)
- Partition tuple (e.g., `ts_day=19797, category="electronics", id_bucket=11`)
- Column-level stats: min/max bounds, null counts, value counts per column
- Record count, file size, file format
- Status: ADDED, EXISTING, or DELETED

A manifest only contains files from a single partition spec. When the partition spec evolves,
new files go into new manifests with the new spec.

### Manifest List

A manifest list is an **Avro file that lists manifests**. It's one level above manifests.

For each manifest, the manifest list stores:
- Manifest file path
- Partition value ranges (min/max across all files in that manifest)
- File counts (how many added, existing, deleted)
- Row counts

### Why two levels?

This creates a tree structure that enables progressively finer filtering:

```
Manifest List
├── Manifest A   (partition range: ts_day 19795-19800)
│     ├── file_01.parquet  {ts_day=19795, min_id=1,  max_id=500}
│     ├── file_02.parquet  {ts_day=19796, min_id=3,  max_id=499}
│     └── file_03.parquet  {ts_day=19797, min_id=2,  max_id=498}
│
├── Manifest B   (partition range: ts_day 19801-19810)
│     ├── file_04.parquet  ...
│     └── file_05.parquet  ...
│
└── Manifest C   (partition range: ts_day 19811-19820)
      ├── file_06.parquet  ...
      └── file_07.parquet  ...
```

When you query `WHERE ts > '2024-03-15'` (ts_day >= 19797):

**Step 1 — Filter manifest list:** Check each manifest's partition range.
Manifest A's range [19795, 19800] overlaps → read it. If a manifest's range was entirely
below 19797, skip it entirely — never open that file.

**Step 2 — Filter manifests:** Inside Manifest A, check each file's partition value.
file_01 (ts_day=19795) → skip. file_02 (ts_day=19796) → skip. file_03 (ts_day=19797) → read.

**Step 3 — Filter by column stats:** Even for files that pass the partition filter, check
column-level min/max bounds. If file_03 has `ts max = 2024-03-15T06:00:00` and your query
needs `ts > 2024-03-15T12:00:00`, skip it too.

Out of thousands of files, only a handful are actually read. This is why Iceberg scan
planning is fast — it's metadata-only filtering before touching any data.

Code: `ManifestWriter` (`core/.../ManifestWriter.java`) writes manifests. `ManifestReader`
(`core/.../ManifestReader.java`) reads them. `ManifestGroup` (`core/.../ManifestGroup.java`)
orchestrates the scan planning flow: read manifest list → filter manifests → filter files.
`ManifestEvaluator` filters manifests using partition ranges. `InclusiveMetricsEvaluator`
filters files using column stats.

## 4. Snapshots

A snapshot represents the **complete state of a table at a point in time**.

Concretely, a snapshot is a pointer to one manifest list. That manifest list contains every
manifest that contributes live data files to the table. If a file isn't reachable through the
snapshot's manifest list, it doesn't exist in that version of the table.

### Every write creates a new snapshot

```
Commit 1: INSERT 3 files
  Snapshot 1 → manifest_list_1
                 └── manifest_A: [file_1, file_2, file_3]

Commit 2: INSERT 2 more files
  Snapshot 2 → manifest_list_2
                 ├── manifest_A: [file_1, file_2, file_3]   ← reused from snapshot 1
                 └── manifest_B: [file_4, file_5]            ← new

Commit 3: DELETE file_2
  Snapshot 3 → manifest_list_3
                 ├── manifest_A': [file_1, file_3]           ← rewritten without file_2
                 └── manifest_B: [file_4, file_5]            ← reused
```

Key properties:
- Each snapshot is **immutable** — once created, it never changes
- New snapshots **reuse manifests** from previous snapshots (appends are cheap)
- The table metadata tracks which snapshot is "current"

### Time travel

Old snapshots are kept around (until expired), and data files are never modified in place.
When you time travel to an older snapshot, you're reading the actual physical files that
existed at that point — not a reconstruction.

```
Physical files on disk:

file_1.parquet   ← written by commit 1, still on disk
file_2.parquet   ← written by commit 1, still on disk
file_3.parquet   ← written by commit 1, still on disk
file_4.parquet   ← written by commit 2, still on disk
file_5.parquet   ← written by commit 2, still on disk

Snapshot 1 sees: file_1, file_2, file_3
Snapshot 2 sees: file_1, file_2, file_3, file_4, file_5
Snapshot 3 sees: file_1, file_3, file_4, file_5
```

Time travel to snapshot 1 reads file_1, file_2, file_3. All physically present.

Even after compaction — say snapshot 4 merges file_1 + file_3 into file_6:

```
Snapshot 3 sees: file_1, file_3, file_4, file_5
Snapshot 4 sees: file_4, file_5, file_6
```

All files (1, 3, 4, 5, 6) exist on disk. Time travel to snapshot 3 still works.

The only thing that breaks time travel is **snapshot expiration**. When you run
`expireSnapshots`, Iceberg removes old snapshots from metadata and deletes data files that
are no longer referenced by any remaining snapshot. Once expired, those files are gone.

### Atomic commits

Committing a write means atomically swapping the "current snapshot" pointer in table
metadata. One pointer change switches the entire table from one complete set of files to
another. Readers that already loaded the old snapshot are unaffected — they continue reading
the old set of files (reader isolation).

### What's in a snapshot

From a real V2 metadata JSON (`core/src/test/resources/TableMetadataV2Valid.json`):

```json
{
  "snapshot-id": 3055729675574597004,
  "parent-snapshot-id": 3051729675574597004,
  "timestamp-ms": 1555100955770,
  "sequence-number": 1,
  "summary": { "operation": "append" },
  "manifest-list": "s3://a/b/2.avro",
  "schema-id": 1
}
```

- `snapshot-id` — unique identifier
- `parent-snapshot-id` — the previous snapshot (forms a chain of history)
- `manifest-list` — the one file that defines everything in this snapshot
- `sequence-number` — monotonically increasing, used to determine which delete files
  apply to which data files
- `operation` — what created it: `append`, `overwrite`, `delete`, or `replace`

Code: `Snapshot` interface is in `api/.../Snapshot.java`. `BaseSnapshot` is in
`core/.../BaseSnapshot.java`. `SnapshotProducer` (`core/.../SnapshotProducer.java`) is the
base class for all operations that create new snapshots.

## Putting It All Together

The full metadata hierarchy:

```
Table Metadata JSON
│   (stores: schemas, partition specs, sort orders, properties, snapshot list)
│
├── current-snapshot-id: 3055729675574597004
│
└── Snapshot 3055729675574597004
      │   (stores: snapshot-id, timestamp, operation, manifest-list path)
      │
      └── Manifest List (s3://a/b/2.avro)
            │   (stores: one row per manifest with partition ranges, file counts)
            │
            ├── Manifest A (s3://a/b/manifest-a.avro)
            │     │   (stores: partition-spec-id, schema)
            │     ├── file_1.parquet  {partition tuple, column stats, size, format}
            │     ├── file_2.parquet  {partition tuple, column stats, size, format}
            │     └── file_3.parquet  {partition tuple, column stats, size, format}
            │
            └── Manifest B (s3://a/b/manifest-b.avro)
                  ├── file_4.parquet  {partition tuple, column stats, size, format}
                  └── file_5.parquet  {partition tuple, column stats, size, format}
```

Reading a table: load metadata → pick snapshot → read manifest list → filter manifests →
filter files → read data.

Writing to a table: write new data files → write new manifest → write new manifest list →
create new snapshot → atomically swap current snapshot pointer.
