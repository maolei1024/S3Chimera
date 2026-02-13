# Local File System Storage Driver

An S3-compatible storage driver based on the local file system.

## Features

- Buckets are mapped to directories
- Objects are mapped to files (supports nested directories as key prefixes)
- Metadata is stored in `.meta` JSON sidecar files
- Supports multipart uploads (temporary file merging)

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `base-path` | String | `/tmp/s3chimera` | No | Storage root directory. Buckets are mapped to subdirectories under this path |

## Configuration Example

```yaml
chimera:
  drivers:
    - name: local-storage
      type: local
      enabled: true
      properties:
        base-path: /data/s3-storage
```
