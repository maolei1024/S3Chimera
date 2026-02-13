# S3 Proxy Driver

Mounts a standard S3-compatible service (AWS S3, MinIO, Aliyun OSS) as a backend.

## Features

- Transparent proxying
- Supports multipart upload proxying
- Compatible with AWS SDK v2

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `endpoint` | String | `null` | No | S3 endpoint URL (uses AWS default if not set) |
| `access-key` | String | `""` | **Yes** | Access Key ID |
| `secret-key` | String | `""` | **Yes** | Secret Access Key |
| `region` | String | `us-east-1` | No | AWS region |
| `path-style` | boolean | `true` | No | Whether to use path-style access (must be `true` for MinIO) |

## Configuration Examples

### MinIO (path-style)

```yaml
chimera:
  drivers:
    - name: s3-minio
      type: s3
      enabled: true
      properties:
        endpoint: http://minio:9000
        access-key: minioadmin
        secret-key: minioadmin
        region: us-east-1
        path-style: true
```

### AWS S3 (virtual-host-style)

```yaml
chimera:
  drivers:
    - name: s3-aws
      type: s3
      enabled: true
      properties:
        access-key: AKIAIOSFODNN7EXAMPLE
        secret-key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
        region: ap-southeast-1
        path-style: false
```

### Aliyun OSS

```yaml
chimera:
  drivers:
    - name: s3-oss
      type: s3
      enabled: true
      properties:
        endpoint: https://oss-cn-hangzhou.aliyuncs.com
        access-key: your-access-key-id
        secret-key: your-access-key-secret
        region: cn-hangzhou
        path-style: false
```
