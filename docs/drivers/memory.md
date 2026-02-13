# Memory Storage Driver

An in-memory S3-compatible storage driver, intended for development testing or temporary caching.

## Features

- Ultra-fast read/write
- Data is non-persistent (lost on restart)
- No external dependencies required

## Configuration Properties

No `properties` configuration is needed.

## Configuration Example

```yaml
chimera:
  drivers:
    - name: memory-driver
      type: memory
      enabled: true
```
