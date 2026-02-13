package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import lombok.extern.slf4j.Slf4j;
import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * SFTP 上传对象处理器
 * Uses file-level locks to prevent concurrent write conflicts
 */
@Slf4j
public class SftpPutObjectHandler implements OperationHandler<PutObjectOperation, S3Object> {

    // 文件级锁，按对象路径进行同步
    private static final ConcurrentHashMap<String, ReentrantLock> FILE_LOCKS = new ConcurrentHashMap<>();

    private ReentrantLock getLock(String path) {
        return FILE_LOCKS.computeIfAbsent(path, k -> new ReentrantLock());
    }

    @Override
    public Mono<S3Object> handle(PutObjectOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String objectPath = ctx.getObjectPath(bucketName, key);

        return operation.getContent()
                .reduce(new ByteArrayOutputStream(), (baos, buffer) -> {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    baos.write(bytes, 0, bytes.length);
                    return baos;
                })
                .flatMap(baos -> Mono.fromCallable(() -> {
                    ReentrantLock lock = getLock(objectPath);
                    lock.lock();
                    try {
                        return doUpload(ctx, bucketName, key, baos.toByteArray(), operation.getContentType(),
                                operation.getMetadata());
                    } finally {
                        lock.unlock();
                        // Clean up lock (avoid memory leak, only when no other threads are waiting)
                        FILE_LOCKS.remove(objectPath, lock);
                    }
                }).subscribeOn(Schedulers.boundedElastic()))
                .doOnError(e -> log.error("SFTP PutObject failed: bucket={}, key={}", bucketName, key, e));
    }

    private S3Object doUpload(SftpDriverContext ctx, String bucketName, String key, byte[] data,
            String contentType, java.util.Map<String, String> metadata) throws Exception {
        try (SftpClient sftpClient = ctx.createSftpClient()) {
            String bucketPath = ctx.getBucketPath(bucketName);

            // 检查 bucket 是否存在
            try {
                var attrs = sftpClient.stat(bucketPath);
                if (!attrs.isDirectory()) {
                    throw new BucketNotFoundException(bucketName);
                }
            } catch (IOException e) {
                throw new BucketNotFoundException(bucketName);
            }

            String objectPath = ctx.getObjectPath(bucketName, key);
            String tempPath = objectPath + "." + UUID.randomUUID() + ".tmp";
            String metadataPath = ctx.getMetadataPath(bucketName, key);

            // 确保父目录存在（数据目录和元数据目录）
            ensureParentDirectories(sftpClient, ctx.getBucketPath(bucketName), key);
            ensureParentDirectoriesForPath(sftpClient, metadataPath);

            try {
                // 上传到临时文件
                try (OutputStream os = sftpClient.write(tempPath,
                        SftpClient.OpenMode.Write,
                        SftpClient.OpenMode.Create,
                        SftpClient.OpenMode.Truncate)) {
                    os.write(data);
                    os.flush();
                }

                // 原子重命名 (兼容不支持 CopyMode 的 SFTP 服务器)
                try {
                    sftpClient.rename(tempPath, objectPath, SftpClient.CopyMode.Atomic,
                            SftpClient.CopyMode.Overwrite);
                } catch (Exception e) {
                    // 某些 SFTP 服务器不支持 CopyMode 选项 (可能抛出 IOException 或 UnsupportedOperationException)
                    // Use delete+rename for compatibility
                    try {
                        sftpClient.remove(objectPath);
                    } catch (Exception ignored) {
                        // 目标文件可能不存在，忽略
                    }
                    sftpClient.rename(tempPath, objectPath);
                }

            } catch (Exception e) {
                // 清理临时文件
                try {
                    sftpClient.remove(tempPath);
                } catch (IOException ignore) {
                }
                throw e;
            }

            // Stat the just-written file to get precise mtime and size for ETag
            SftpClient.Attributes attrs = sftpClient.stat(objectPath);
            String etag = generateFallbackETag(data); // Use actual MD5 as ETag

            // 写入 Sidecar 元数据文件（隔离在 meta 目录）
            SidecarMetadata sidecar = SidecarMetadata.builder()
                    .etag(etag)
                    .contentType(contentType)
                    .userMetadata(metadata)
                    .size(attrs.getSize())
                    .lastModified(attrs.getModifyTime().toMillis())
                    .build();

            try (OutputStream os = sftpClient.write(metadataPath,
                    SftpClient.OpenMode.Write,
                    SftpClient.OpenMode.Create,
                    SftpClient.OpenMode.Truncate)) {
                os.write(JsonUtils.toJsonBytes(sidecar));
                os.flush();
            } catch (IOException e) {
                log.warn("Failed to write sidecar metadata for {}: {}", key, e.getMessage());
            }

            // 强制刷新目录缓存 - 通过 stat 父目录确保文件可见
            // This is a synchronization point, ensuring subsequent readDir/listObjects can see new files
            try {
                String parentDir = objectPath.contains("/")
                        ? objectPath.substring(0, objectPath.lastIndexOf('/'))
                        : bucketPath;
                sftpClient.stat(parentDir);
            } catch (IOException e) {
                log.debug("Failed to stat parent directory: {}", e.getMessage());
            }

            return S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(attrs.getSize())
                    .etag("\"" + etag + "\"")
                    .lastModified(attrs.getModifyTime().toInstant())
                    .contentType(contentType)
                    .userMetadata(metadata)
                    .build();
        }
    }

    private String generateFallbackETag(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(data);
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            return "d41d8cd98f00b204e9800998ecf8427e"; // empty content hash
        }
    }

    private void ensureParentDirectories(SftpClient sftpClient, String basePath,
            String key) throws IOException {
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentPath = key.substring(0, lastSlash);
            String[] parts = parentPath.split("/");
            StringBuilder path = new StringBuilder(basePath);

            for (String part : parts) {
                if (part.isEmpty())
                    continue;
                path.append("/").append(part);
                String dirPath = path.toString();
                try {
                    sftpClient.stat(dirPath);
                } catch (IOException e) {
                    sftpClient.mkdir(dirPath);
                }
            }
        }
    }

    /**
     * 确保给定路径的父目录存在
     */
    private void ensureParentDirectoriesForPath(SftpClient sftpClient, String fullPath) throws IOException {
        int lastSlash = fullPath.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentDir = fullPath.substring(0, lastSlash);
            // Recursively create all parent directories
            ensureDirectoryExists(sftpClient, parentDir);
        }
    }

    private void ensureDirectoryExists(SftpClient sftpClient, String dirPath) throws IOException {
        try {
            sftpClient.stat(dirPath);
        } catch (IOException e) {
            // 父目录可能也不存在，先递归创建
            int lastSlash = dirPath.lastIndexOf('/');
            if (lastSlash > 0) {
                ensureDirectoryExists(sftpClient, dirPath.substring(0, lastSlash));
            }
            try {
                sftpClient.mkdir(dirPath);
            } catch (IOException ignored) {
                // May have been concurrently created
            }
        }
    }

    @Override
    public Class<PutObjectOperation> getOperationType() {
        return PutObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
