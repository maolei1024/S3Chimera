# SFTP Storage Driver

Exposes an SFTP server as an S3-compatible interface.

## Features

- Built on Apache MINA SSHD
- Supports password and key-based authentication
- Directories are mapped to Buckets (Buckets must be subdirectories under `base-path`)

## Limitations

- Multipart uploads are not supported
- Range reads are not supported

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `host` | String | `localhost` | **Yes** | SFTP server address |
| `port` | int | `22` | No | SFTP port |
| `username` | String | `""` | **Yes** | Username |
| `password` | String | `""` | Conditional | Password (required for password authentication) |
| `private-key` | String | `""` | Conditional | Private key file path (required for key-based authentication) |
| `base-path` | String | `/` | No | Root path. Buckets are mapped to subdirectories under this path |

> **Note**: `password` and `private-key` are mutually exclusive, corresponding to password authentication and key-based authentication respectively.

## Configuration Examples

### Password Authentication

```yaml
chimera:
  drivers:
    - name: sftp-password
      type: sftp
      enabled: true
      properties:
        host: sftp.example.com
        port: 22
        username: user
        password: pass
        base-path: /data
```

### Key-Based Authentication

```yaml
chimera:
  drivers:
    - name: sftp-key
      type: sftp
      enabled: true
      properties:
        host: sftp.example.com
        port: 22
        username: user
        private-key: /home/user/.ssh/id_rsa
        base-path: /upload
```
