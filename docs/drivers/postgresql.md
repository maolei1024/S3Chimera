# PostgreSQL Storage Driver

An S3-compatible storage driver using PostgreSQL as the backend.

## Features

- Separate storage for metadata and data
- Supports database and table sharding (Hash Slot strategy)
- R2DBC async driver compatible
- Supports BYTEA binary storage and JSONB metadata

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `url` | String | - | **Yes** | Metadata database R2DBC URL, format: `r2dbc:postgresql://host:port/database` |
| `username` | String | `""` | No | Metadata database username |
| `password` | String | `""` | No | Metadata database password |
| `data-sources` | List | `null` | No | Data source list, each item contains `name`, `url`, `username`, `password` |
| `table-shard-count` | int | `16` | No | Number of sharded tables per database |
| `total-hash-slots` | int | `1024` | No | Total number of hash slots |
| `chunk-size` | String | `"4MB"` | No | Chunk size, supports `MB`/`KB`/plain number (bytes) |
| `pool-size` | int | `10` | No | Connection pool size |
| `write-concurrency` | int | `2` | No | Parallel write concurrency |
| `auto-create-schema` | boolean | `true` | No | Whether to auto-create table schemas |

> **Note**: If `data-sources` is not configured, chunk data is stored in the metadata database (single-database mode).

## Configuration Examples

### Single-Database Mode

```yaml
chimera:
  drivers:
    - name: postgres-driver
      type: postgresql
      enabled: true
      properties:
        url: r2dbc:postgresql://localhost:5432/s3chimera
        username: postgres
        password: password
```

### Multi-Database Sharded Mode

```yaml
chimera:
  drivers:
    - name: postgres-driver
      type: postgresql
      enabled: true
      properties:
        url: r2dbc:postgresql://localhost:5432/s3chimera_meta
        username: postgres
        password: password
        data-sources:
          - name: shard-1
            url: r2dbc:postgresql://node1:5432/s3chimera_data_1
            username: postgres
            password: password
          - name: shard-2
            url: r2dbc:postgresql://node2:5432/s3chimera_data_2
            username: postgres
            password: password
        table-shard-count: 64
        total-hash-slots: 1024
        chunk-size: 5MB
        pool-size: 10
        write-concurrency: 2
        auto-create-schema: true
```
