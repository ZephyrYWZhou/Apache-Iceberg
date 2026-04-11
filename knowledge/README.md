# Knowledge Branch

Personal notes for learning the Apache Iceberg Java codebase.

## How this differs from existing docs

- `docs/` = user-facing guides (how to use Iceberg from Spark/Flink/Java)
- `format/` = the spec (what the format defines)
- `AGENTS.md` = coding rules (what reviewers enforce)
- `knowledge/` = **code-level maps and traces** (how the implementation actually works)

## Files

| File | Purpose |
|------|---------|
| [core-concepts.md](core-concepts.md) | Partition specs, partitions, manifests, manifest lists, snapshots — with examples |
| [operations-map.md](operations-map.md) | Every Table/Catalog operation mapped to its core implementation class |
| [metadata-consistency.md](metadata-consistency.md) | How metadata-only changes stay consistent with data (field IDs, projection, partition evolution) |
| [cloud-modules.md](cloud-modules.md) | Cloud provider modules — what they provide (FileIO, Catalog, KMS), how they're loaded, per-module summary |
