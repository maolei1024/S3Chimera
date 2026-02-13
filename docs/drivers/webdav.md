# WebDAV Storage Driver

Uses a WebDAV server (such as Nextcloud, Jianguoyun) as the backend storage.

## Features

- Built on Sardine
- Maps WebDAV directories to Buckets
- Supports basic authentication

## Configuration Properties

| Property Key | Type | Default | Required | Description |
|-------------|------|---------|----------|-------------|
| `url` | String | `""` | **Yes** | WebDAV server endpoint URL |
| `username` | String | `""` | **Yes** | Username |
| `password` | String | `""` | **Yes** | Password |

## Configuration Example

```yaml
chimera:
  drivers:
    - name: webdav-driver
      type: webdav
      enabled: true
      properties:
        url: https://dav.jianguoyun.com/dav/
        username: user@example.com
        password: app-password
```
