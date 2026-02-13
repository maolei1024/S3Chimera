# MongoDB Storage Driver

An S3-compatible storage driver using MongoDB as the backend.

## Features

- Supports primary/secondary database mode
- Metadata and data can be stored in separate databases
- Automatic chunking (to work around the 16MB document size limit)
- Built on MongoDB Reactive Streams async driver

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `uri` | String | `mongodb://localhost:27017` | No | MongoDB connection URI |
| `database` | String | `chimera_meta` | No | Metadata database name |
| `username` | String | `""` | No | Username (if not included in the URI) |
| `password` | String | `""` | No | Password (if not included in the URI) |
| `data-sources` | List | `null` | No | Data source list, each item contains `name`, `uri`, `database`, `username`, `password` |
| `chunk-size` | String | `"4MB"` | No | Chunk size, supports `MB`/`KB`/plain number (bytes) |
| `write-concurrency` | int | `2` | No | Parallel write concurrency |
| `auto-create-schema` | boolean | `true` | No | Whether to auto-create collections and indexes |

> **Note**: If `data-sources` is not configured, all data is stored in the metadata database (single-database mode).

## Configuration Examples

### Single-Database Mode

```yaml
chimera:
  drivers:
    - name: mongo-driver
      type: mongodb
      enabled: true
      properties:
        uri: mongodb://localhost:27017
        database: chimera_meta
```

### Authenticated + Multi-Database Sharded Mode

```yaml
chimera:
  drivers:
    - name: mongo-driver
      type: mongodb
      enabled: true
      properties:
        uri: mongodb://localhost:27017
        database: chimera_meta
        username: admin
        password: secret
        data-sources:
          - name: shard-1
            uri: mongodb://mongo-shard1:27017
            database: chimera_data_1
            username: admin
            password: secret
          - name: shard-2
            uri: mongodb://mongo-shard2:27017
            database: chimera_data_2
        chunk-size: 5MB
        write-concurrency: 2
        auto-create-schema: true
```
