# How Metadata-Only Changes Stay Consistent With Data

**Question:** Schema evolution and partition evolution are metadata-only changes — no data files
are rewritten. How does the changed metadata stay consistent with the underlying data?

**Answer:** Field IDs. Every column is tracked by a unique integer ID that is never reused.
Data files store columns tagged with these IDs. Readers match columns by ID, not by name or
position. This decouples the schema from the physical layout of data files.

## 1. Field IDs are never reused

`TableMetadata` stores `lastColumnId`, a monotonically increasing counter. When `SchemaUpdate`
adds a column, it calls `assignNewColumnId()` which increments this counter:

```
SchemaUpdate.java:478-481
─────────────────────────
private int assignNewColumnId() {
    int next = lastColumnId + 1;
    this.lastColumnId = next;
    return next;
}
```

Every `addColumn` call goes through this (line 166), and nested types get fresh IDs too via
`TypeUtil.assignFreshIds(type, this::assignNewColumnId)` (line 179).

The counter is persisted in table metadata JSON as `last-column-id` and exposed via
`TableMetadata.lastColumnId()` (line 428). On initial table creation, it starts at 0 and
increments via `AtomicInteger` (line 119-121).

Because the counter only goes up, a dropped column's ID is never assigned to a new column.

## 2. Data files store columns by field ID

Each file format embeds Iceberg field IDs in its own metadata:

| Format  | Where IDs are stored | Code reference |
|---------|---------------------|----------------|
| Parquet | Thrift field IDs on each column | Parquet spec `parquet.thrift` field_id |
| Avro    | `"field-id"` JSON property on each field | `AvroSchemaUtil.java:46` — `FIELD_ID_PROP = "field-id"` |
| ORC     | `"iceberg.id"` type attribute on each column | `ORCSchemaUtil.java:68` — `ICEBERG_ID_ATTRIBUTE = "iceberg.id"` |

## 3. Readers project by field ID, not name or position

When reading, Iceberg collects the set of field IDs from the expected (current) schema, then
selects only matching columns from the file's schema.

**Parquet** — `ParquetSchemaUtil.pruneColumns()` (line 130):
```
Set<Integer> selectedIds = TypeUtil.getProjectedIds(expectedSchema);
return TypeWithSchemaVisitor.visit(
    expectedSchema.asStruct(), fileSchema, new PruneColumns(selectedIds));
```
The `PruneColumns` visitor walks the file schema and keeps only columns whose field ID is in
`selectedIds`.

**Avro** — `BuildAvroProjection.java` walks the expected schema and matches fields by ID
against the file schema. If a field exists in the file, it's projected. If not, see below.

**ORC** — `ORCSchemaUtil` reads `iceberg.id` attributes to map ORC columns to Iceberg field
IDs, then Iceberg renames ORC columns to match the expected schema so ORC's name-based
evolution handles the rest.

## 4. Missing columns return null (or a default)

When the expected schema has a column that doesn't exist in the data file (e.g., a column
added after the file was written), the reader creates a placeholder that returns null.

**Avro** — `BuildAvroProjection.java:109-116`:
```
// Create a field that will be defaulted to null.
Schema.Field newField = new Schema.Field(
    fieldName + "_r" + field.fieldId(),
    AvroSchemaUtil.toOption(AvroSchemaUtil.convert(field.type())),
    null,
    JsonProperties.NULL_VALUE);   // ← default is null
```

**Parquet/ORC** — similar: if a column ID isn't in the file, the reader returns null values
for that column. If the schema defines an `initial-default`, that value is used instead.

## 5. Partition evolution — same principle, different mechanism

Each manifest file stores its own partition spec as Avro key-value metadata:

**ManifestWriter.java:275-277:**
```
.meta("schema", SchemaParser.toJson(spec.schema()))
.meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
.meta("partition-spec-id", String.valueOf(spec.specId()))
```

**ManifestReader.java:147** reads it back:
```
String specProperty = metadata.get("partition-spec-id");
```

This means old manifests written with an old partition spec are self-contained. When the
partition spec evolves, new manifests use the new spec. During scan planning, Iceberg converts
query predicates into partition predicates independently for each spec version.

## Why this works

The invariant is: **data files are immutable and self-describing (via field IDs), so metadata
can evolve freely as long as the ID mapping is preserved.**

| Operation | What changes | Why data stays consistent |
|-----------|-------------|--------------------------|
| Add column | New ID assigned | Old files don't have this ID → null returned |
| Drop column | ID retired | Old files still have it → projection skips it |
| Rename column | Same ID, new name | Readers match by ID, not name |
| Reorder columns | Same IDs | Readers match by ID, not position |
| Promote type | Same ID | Readers widen value at read time |
| Change partition spec | New spec for new manifests | Old manifests keep old spec, each is self-contained |
