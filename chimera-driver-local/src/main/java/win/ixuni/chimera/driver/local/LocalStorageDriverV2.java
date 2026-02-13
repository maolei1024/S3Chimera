package win.ixuni.chimera.driver.local;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.driver.CleanableDriver;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;
import win.ixuni.chimera.driver.local.handler.bucket.*;
import win.ixuni.chimera.driver.local.handler.multipart.*;
import win.ixuni.chimera.driver.local.handler.object.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/**
 * 本地文件系统存储驱动 V2
 * <p>
 * 将本地文件系统暴露为 S3 兼容接口。
 * Buckets are mapped to directories, objects are mapped to files.
 */
@Slf4j
public class LocalStorageDriverV2 extends AbstractStorageDriverV2 implements CleanableDriver {

    @Getter
    private final DriverConfig config;

    private final LocalDriverContext driverContext;

    public LocalStorageDriverV2(DriverConfig config) {
        this.config = config;

        String basePath = config.getString("base-path", "/tmp/s3chimera");
        this.driverContext = LocalDriverContext.builder()
                .config(config)
                .basePath(Path.of(basePath))
                .build();

        registerHandlers();
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new LocalCreateBucketHandler());
        getHandlerRegistry().register(new LocalDeleteBucketHandler());
        getHandlerRegistry().register(new LocalBucketExistsHandler());
        getHandlerRegistry().register(new LocalListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new LocalPutObjectHandler());
        getHandlerRegistry().register(new LocalGetObjectHandler());
        getHandlerRegistry().register(new LocalHeadObjectHandler());
        getHandlerRegistry().register(new LocalDeleteObjectHandler());
        getHandlerRegistry().register(new LocalListObjectsHandler());
        getHandlerRegistry().register(new LocalListObjectsV2Handler());
        getHandlerRegistry().register(new LocalCopyObjectHandler());
        getHandlerRegistry().register(new LocalDeleteObjectsHandler());
        getHandlerRegistry().register(new LocalGetObjectRangeHandler());

        // Multipart handlers (6)
        getHandlerRegistry().register(new LocalCreateMultipartUploadHandler());
        getHandlerRegistry().register(new LocalUploadPartHandler());
        getHandlerRegistry().register(new LocalCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new LocalAbortMultipartUploadHandler());
        getHandlerRegistry().register(new LocalListPartsHandler());
        getHandlerRegistry().register(new LocalListMultipartUploadsHandler());

        log.info("Registered {} operation handlers for local driver", getHandlerRegistry().size());
    }

    @Override
    public DriverContext getDriverContext() {
        return driverContext;
    }

    @Override
    public String getDriverType() {
        return LocalDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        log.info("Initializing local filesystem driver: {} at {}",
                config.getName(), driverContext.getBasePath());

        return Mono.fromRunnable(() -> {
            try {
                Files.createDirectories(driverContext.getBasePath());
                // 从磁盘恢复分片上传状态
                driverContext.restoreMultipartStates();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize local driver at: " + driverContext.getBasePath(), e);
            }
        });
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down local filesystem driver: {}", config.getName());
        return Mono.empty();
    }

    // ============ CleanableDriver 实现 ============

    @Override
    public Mono<Long> cleanupOrphanChunks(int olderThanHours) {
        return Mono.fromCallable(() -> {
            long cleaned = 0;
            Path dataRoot = driverContext.getDataRoot();
            if (!Files.exists(dataRoot)) {
                return cleaned;
            }

            Instant cutoff = Instant.now().minus(Duration.ofHours(olderThanHours));

            // Iterate all bucket .multipart directories
            var buckets = Files.list(dataRoot)
                    .filter(Files::isDirectory)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .toList();

            for (Path bucketDir : buckets) {
                Path multipartDir = bucketDir.resolve(LocalFileUtils.MULTIPART_DIR);
                if (!Files.exists(multipartDir)) {
                    continue;
                }

                var uploadDirs = Files.list(multipartDir)
                        .filter(Files::isDirectory)
                        .toList();

                for (Path uploadDir : uploadDirs) {
                    String uploadId = uploadDir.getFileName().toString();
                    // If the upload state is not in memory, it is an orphan
                    if (!driverContext.getMultipartUploads().containsKey(uploadId)) {
                        var attrs = Files.readAttributes(uploadDir,
                                java.nio.file.attribute.BasicFileAttributes.class);
                        if (attrs.creationTime().toInstant().isBefore(cutoff)) {
                            LocalFileUtils.deleteDirectoryRecursively(uploadDir);
                            cleaned++;
                            log.info("Cleaned orphan multipart directory: {}", uploadDir);
                        }
                    }
                }
            }

            return cleaned;
        });
    }

    @Override
    public Mono<Long> cleanupExpiredUploads(int olderThanHours) {
        return Mono.fromCallable(() -> {
            long cleaned = 0;
            Instant cutoff = Instant.now().minus(Duration.ofHours(olderThanHours));

            var expiredUploads = driverContext.getMultipartUploads().values().stream()
                    .filter(state -> state.getInitiated().isBefore(cutoff))
                    .toList();

            for (var state : expiredUploads) {
                try {
                    // 删除临时分片目录
                    Path multipartDir = driverContext.getMultipartPath(
                            state.getBucketName(), state.getUploadId());
                    LocalFileUtils.deleteDirectoryRecursively(multipartDir);

                    // 清理状态（内存 + 磁盘）
                    driverContext.removeMultipartState(state.getUploadId());

                    cleaned++;
                    log.info("Cleaned expired multipart upload: {} for key: {}",
                            state.getUploadId(), state.getKey());
                } catch (Exception e) {
                    log.warn("Failed to clean expired upload: {}", state.getUploadId(), e);
                }
            }

            return cleaned;
        });
    }

    @Override
    public Mono<CleanupResult> performCleanup(int olderThanHours) {
        return cleanupExpiredUploads(olderThanHours)
                .zipWith(cleanupOrphanChunks(olderThanHours))
                .map(tuple -> new CleanupResult(tuple.getT1(), tuple.getT2()));
    }
}
