# S3Chimera

S3Chimera is an **S3-compatible storage gateway**.

It allows you to access various heterogeneous storage backends through the standard S3 protocol (using tools such as AWS CLI, MinIO Client, S3 Browser, etc.), including local file systems, databases (MySQL/PostgreSQL/MongoDB), other object storage services (S3/MinIO), and network protocol storage (SFTP/WebDAV).

The original purpose of this project is for personal use on a VPS to convert various free services available on the internet into an S3-compatible interface. It is not intended to provide mature, production-grade commercial functionality. The primary use case is to extend server disk capacity in conjunction with rclone.



## Supported Storage Drivers

S3Chimera currently supports the following storage backends. Click the links to view detailed configuration documentation:

| Driver Type | Description | Documentation |
|-------------|-------------|---------------|
| **Local** | Local file system | [View Docs](docs/drivers/local.md) |
| **MySQL** | Stores metadata and file chunks in MySQL, supports database/table sharding | [View Docs](docs/drivers/mysql.md) |
| **PostgreSQL** | PostgreSQL storage with Reactive async IO support | [View Docs](docs/drivers/postgresql.md) |
| **MongoDB** | MongoDB backend with GridFS-style chunking | [View Docs](docs/drivers/mongodb.md) |
| **S3 Proxy** | Proxies other S3-compatible services (AWS S3, MinIO, OSS) | [View Docs](docs/drivers/s3-proxy.md) |
| **SFTP** | Maps an SFTP server as a storage bucket | [View Docs](docs/drivers/sftp.md) |
| **WebDAV** | Connects to Nextcloud, Jianguoyun, and other WebDAV services | [View Docs](docs/drivers/webdav.md) |
| **Memory** | In-memory storage for testing or temporary caching only | [View Docs](docs/drivers/memory.md) |

## Quick Start

### Option 1: Build JAR from Source

```bash
# 1. Clone the project
git clone https://github.com/xuni2048/S3Chimera.git
cd S3Chimera

# 2. Prepare the configuration file
cp application.example.yaml application.yaml
# Edit application.yaml to configure the drivers you need

# 3. Build the JAR
./gradlew build -x test

# 4. Start the application
java -jar chimera-server/build/libs/chimera-server-0.0.1-SNAPSHOT.jar
```

### Option 2: Start with Docker

```bash
# Pull the image
docker pull xuni2048/s3chimera:latest

# Start the container (mount the configuration file)
docker run -d \
  --name s3chimera \
  -p 9000:9000 \
  -v $(pwd)/application.yaml:/app/application.yaml \
  xuni2048/s3chimera:latest
```

### Verify the Service

Test the connection using AWS CLI:

```bash
aws s3 ls --endpoint-url http://localhost:9000
```
