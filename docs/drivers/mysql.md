# MySQL Storage Driver

An S3-compatible storage driver using MySQL as the backend.

## Features

- Separate storage for metadata and data
- Supports database and table sharding (similar to Redis Cluster slot-based sharding)
- R2DBC async driver compatible
- Supports single-database mode, simple mode (multi-database with shared credentials), and independent mode (multi-database with independent credentials)

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `url` | String | - | **Yes** | Metadata database R2DBC URL, format: `r2dbc:mysql://host:port/database` |
| `username` | String | `""` | No | Metadata database username (also used as default data database username in simple mode) |
| `password` | String | `""` | No | Metadata database password (also used as default data database password in simple mode) |
| `data-urls` | String | `""` | No | Simple mode: comma-separated data database URLs |
| `data-sources` | List | `null` | No | Independent mode: data source list, each item contains `name`, `url`, `username`, `password` |
| `table-shard-count` | int | `16` | No | Number of sharded tables per database (1-256) |
| `total-hash-slots` | int | `1024` | No | Total number of hash slots |
| `chunk-size` | String | `"4MB"` | No | Chunk size, supports `MB`/`KB`/plain number (bytes) |
| `pool-size` | int | `10` | No | Connection pool size per database |
| `write-concurrency` | int | `2` | No | Parallel write concurrency |
| `read-concurrency` | int | `4` | No | Parallel read concurrency |
| `auto-create-schema` | boolean | `true` | No | Whether to auto-create table schemas |

> **Note**: `data-sources` and `data-urls` are mutually exclusive. If `data-sources` is configured, `data-urls` will be ignored.  
> If neither is configured, chunk data is stored in the metadata database (single-database mode).

## Configuration Examples

### Single-Database Mode (Minimal Configuration)

```yaml
chimera:
  drivers:
    - name: mysql-driver
      type: mysql
      enabled: true
      properties:
        url: r2dbc:mysql://localhost:3306/s3chimera
        username: root
        password: password
```

### Simple Mode (data-urls, Multi-Database with Shared Credentials)

```yaml
chimera:
  drivers:
    - name: mysql-driver
      type: mysql
      enabled: true
      properties:
        url: r2dbc:mysql://localhost:3306/s3chimera_meta
        username: root
        password: password
        data-urls: >-
          r2dbc:mysql://localhost:3306/s3chimera_data_0,
          r2dbc:mysql://localhost:3306/s3chimera_data_1
        chunk-size: 4MB
        pool-size: 10
```

### Independent Mode (data-sources, Per-Database Independent Credentials)

```yaml
chimera:
  drivers:
    - name: mysql-driver
      type: mysql
      enabled: true
      properties:
        url: r2dbc:mysql://localhost:3306/s3chimera_meta
        username: root
        password: password
        data-sources:
          - name: shard-1
            url: r2dbc:mysql://node1:3306/s3chimera_data_1
            username: root
            password: password
          - name: shard-2
            url: r2dbc:mysql://node2:3306/s3chimera_data_2
            username: root
            password: password
        table-shard-count: 64
        total-hash-slots: 1024
        chunk-size: 4MB
        pool-size: 10
        write-concurrency: 2
        read-concurrency: 4
        auto-create-schema: true
```
