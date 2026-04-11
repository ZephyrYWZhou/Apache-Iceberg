# Cloud Modules

Cloud modules are **not** engine integrations. Engines (Spark, Flink, Trino) are compute
systems that live in `spark/`, `flink/`, etc. Cloud modules solve a different problem:
**where do the files physically live, and how do you authenticate to access them?**

## What Cloud Modules Provide

Iceberg core defines three cloud-agnostic abstractions. Each cloud module implements one
or more of these using that provider's SDK:

```
                        ┌─────────────────────────────┐
                        │     Compute Engine           │
                        │   (Spark, Flink, Trino)      │
                        └──────────┬──────────────────┘
                                   │
                        ┌──────────▼──────────────────┐
                        │     Iceberg Core             │
                        │  (table format, metadata,    │
                        │   scan planning, commits)    │
                        └──────────┬──────────────────┘
                                   │
                    ┌──────────────┼──────────────────┐
                    │              │                   │
              ┌─────▼─────┐ ┌─────▼─────┐     ┌──────▼──────┐
              │  FileIO    │ │  Catalog   │     │  KMS        │
              │  (storage) │ │  (where is │     │  (encryption│
              │            │ │  metadata?)│     │   keys)     │
              └─────┬──────┘ └─────┬─────┘     └──────┬──────┘
                    │              │                   │
        ════════════╪══════════════╪═══════════════════╪════════
        Cloud SDK   │              │                   │
        boundary    │              │                   │
                    ▼              ▼                   ▼
              S3 / GCS /     Glue / DynamoDB /    AWS KMS /
              ADLS / OSS     BigQuery / ECS       GCP KMS /
                                                  Key Vault
```

1. **FileIO** — "how do I read/write bytes at a location?" Core calls `inputFile.newStream()`
   and `outputFile.create()` without knowing whether the bytes are on S3, GCS, ADLS, or local
   disk.

2. **Catalog** — "where do I find the current metadata pointer for a table?" Core needs to
   atomically swap metadata pointers on commit. Different backends provide different atomicity
   guarantees (DynamoDB conditional writes, Glue version checks, ECS ETags).

3. **KeyManagementClient** — "how do I encrypt/decrypt data encryption keys?" Optional, for
   tables with encryption enabled.

## How the Right Module Gets Loaded

Cloud modules are loaded at runtime by class name — core never imports them directly.

### FileIO: auto-detected by URI scheme

`ResolvingFileIO` (`core/.../io/ResolvingFileIO.java`) maps URI schemes to implementations:

| URI scheme | FileIO class | Module |
|-----------|-------------|--------|
| `s3://`, `s3a://`, `s3n://` | `S3FileIO` | `aws/` |
| `gs://` | `GCSFileIO` | `gcp/` |
| `abfs://`, `abfss://`, `wasb://`, `wasbs://` | `ADLSFileIO` | `azure/` |
| anything else | `HadoopFileIO` | `core/` (fallback) |

Users can also set `io-impl` explicitly in catalog properties to override auto-detection.

### Catalog: always explicitly configured

```properties
# AWS Glue
catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog

# DynamoDB
catalog-impl=org.apache.iceberg.aws.dynamodb.DynamoDbCatalog

# Snowflake (read-only)
catalog-impl=org.apache.iceberg.snowflake.SnowflakeCatalog

# BigQuery Metastore
catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog

# REST catalog (cloud-agnostic)
catalog-impl=org.apache.iceberg.rest.RESTCatalog
```

The catalog and FileIO choices are independent — you can use a REST catalog with S3FileIO,
or a Glue catalog with GCSFileIO. They're separate concerns.

## Per-Module Summary

### AWS (`aws/`) — ~45 main classes

The largest cloud module. Provides everything: two catalogs, FileIO, auth, signing, and KMS.

**Catalogs:**
- `GlueCatalog` — full catalog backed by AWS Glue Data Catalog. Maps namespaces to Glue
  databases, tables to Glue tables. Uses Glue's `UpdateTable` with version checking for
  optimistic concurrency. Supports LakeFormation integration.
- `DynamoDbCatalog` — full catalog backed by a single DynamoDB table. Uses conditional
  writes for atomic commits. Includes a global secondary index for table listing.
- `DynamoDbLockManager` — distributed lock manager using DynamoDB with lease-based locking
  and heartbeat renewal.

**FileIO:**
- `S3FileIO` — primary FileIO for S3. Supports bulk delete (batched to 1000 per request),
  multipart upload, per-prefix client configuration, storage credentials, and recovery
  operations.
- `S3InputStream` — seekable reads via `GetObject` with range headers. Retry with failsafe
  for SSL/socket exceptions.
- `S3OutputStream` — multipart upload writes. Buffers locally (memory or staging files),
  uploads parts in parallel. Supports MD5 checksums, SSE, tagging, ACLs.
- `S3FileIOProperties` — 50+ configuration properties covering endpoints, path-style access,
  SSE (SSE-S3, SSE-KMS, SSE-C), ACLs, multipart settings, signer config, retry policies,
  S3 Access Grants, Analytics Accelerator, and more.

**Auth & Signing:**
- `RESTSigV4AuthManager` / `RESTSigV4AuthSession` — adds SigV4 signing to REST catalog
  requests.
- `VendedCredentialsProvider` — fetches temporary AWS credentials from a REST catalog's
  `/credentials` endpoint.
- `AssumeRoleAwsClientFactory` — creates AWS clients using STS AssumeRole.
- `LakeFormationAwsClientFactory` — extends AssumeRole to use LakeFormation-vended
  credentials for S3/KMS when a table is registered with LakeFormation.
- `S3V4RestSignerClient` — delegates S3 request signing to a remote REST catalog endpoint.

**Key Management:**
- `AwsKeyManagementClient` — wraps AWS KMS for wrap/unwrap/generate data key operations.

**Client Infrastructure:**
- `AwsClientFactory` / `AwsClientFactories` — pluggable factory for creating S3, Glue,
  DynamoDB, KMS clients. Loaded by class name from properties.
- `HttpClientCache` — reference-counted cache for shared `SdkHttpClient` instances.

### GCP (`gcp/`) — ~15 main classes

FileIO, auth, and KMS. No catalog (uses REST catalog or BigQuery module).

**FileIO:**
- `GCSFileIO` — FileIO for Google Cloud Storage. Bulk delete in batches, prefix listing,
  vended credential support with background refresh.
- `GCSInputStream` — seekable reads via GCS `ReadChannel`. Supports range reads.
- `GCSOutputStream` — writes via GCS `WriteChannel`. Supports encryption and user-project
  options.
- `GcsInputStreamWrapper` — adapter for GCS analytics-core library (vectored reads).

**Auth:**
- `GoogleAuthManager` / `GoogleAuthSession` — loads credentials from file, JSON, or
  Application Default Credentials. Injects Bearer tokens into HTTP headers.
- `OAuth2RefreshCredentialsHandler` — refreshes tokens from REST catalog `/credentials`.

**Key Management:**
- `GcpKeyManagementClient` — wraps Google Cloud KMS.

### Azure (`azure/`) — ~13 main classes

FileIO, auth, and KMS. No catalog (uses REST catalog).

**FileIO:**
- `ADLSFileIO` — FileIO for Azure Data Lake Storage Gen2. Handles `abfs://` and `wasbs://`
  URIs. Caches `DataLakeFileSystemClient` per account+container.
- `ADLSInputStream` — seekable reads with range requests. Supports positional reads.
- `ADLSOutputStream` — buffered writes via `DataLakeFileClient.getOutputStream()`.

**Auth:**
- `AdlsTokenCredentialProvider` — SPI interface for custom Azure `TokenCredential` providers.
- `VendedAdlsCredentialProvider` — fetches SAS tokens from REST catalog, caches per account.
- `VendedAzureSasCredentialPolicy` — HTTP pipeline policy injecting vended SAS tokens.

**Key Management:**
- `AzureKeyManagementClient` — wraps Azure Key Vault (RSA-OAEP-256).

### Snowflake (`snowflake/`) — 7 classes, read-only

- `SnowflakeCatalog` — **read-only** catalog. Discovers Iceberg tables via Snowflake JDBC
  (`SHOW ICEBERG TABLES`, `SYSTEM$GET_ICEBERG_TABLE_INFORMATION`). All write operations
  throw `UnsupportedOperationException`.
- `JdbcSnowflakeClient` — JDBC-based implementation that executes Snowflake SQL commands.
- `SnowflakeTableMetadata` — translates Snowflake's path schemes (`azure://` → `wasbs://`,
  `gcs://` → `gs://`) to Iceberg-compatible URIs.
- No FileIO — uses `ResolvingFileIO` to auto-detect based on the metadata location scheme.

### BigQuery (`bigquery/`) — 6 classes

- `BigQueryMetastoreCatalog` — full catalog using BigQuery Metastore. Maps namespaces to
  BQ datasets, tables to BQ tables with `ExternalCatalogTableOptions`. Single-level
  namespaces only. Rename unsupported.
- `BigQueryTableOperations` — ETag-based optimistic concurrency. Populates Hive-style
  statistics (numFiles, numRows, totalSize).
- No FileIO — uses GCSFileIO or ResolvingFileIO.

### Aliyun (`aliyun/`) — 10 classes

- `OSSFileIO` — FileIO for Alibaba Cloud OSS.
- Staging-file-based writes (write to local file, upload on close).
- Supports static credentials, STS tokens, and RRSA (OIDC pod identity).
- No catalog.

### Dell (`dell/`) — 13 classes

- `EcsCatalog` — full catalog backed by Dell EMC ECS (S3-compatible). Stores metadata as
  S3 objects with `.table`/`.namespace` suffixes. Uses ETags for optimistic concurrency.
- `EcsFileIO` — FileIO using Dell ECS append API.

## Common Pattern

Every cloud module follows the same structure:

| Component | Purpose | Examples |
|-----------|---------|---------|
| `XxxFileIO` | Implements `FileIO` / `DelegateFileIO` | `S3FileIO`, `GCSFileIO`, `ADLSFileIO` |
| `XxxInputStream` | Implements `SeekableInputStream` | `S3InputStream`, `GCSInputStream` |
| `XxxOutputStream` | Implements `PositionOutputStream` | `S3OutputStream`, `GCSOutputStream` |
| `XxxProperties` | Configuration holder (`Serializable`) | `S3FileIOProperties`, `GCPProperties` |
| `XxxClientFactory` | Creates cloud SDK clients | `AwsClientFactory`, `AliyunClientFactory` |
| `XxxCatalog` (optional) | Implements `BaseMetastoreCatalog` | `GlueCatalog`, `DynamoDbCatalog` |
| `XxxKeyManagementClient` (optional) | Implements `KeyManagementClient` | `AwsKeyManagementClient` |
