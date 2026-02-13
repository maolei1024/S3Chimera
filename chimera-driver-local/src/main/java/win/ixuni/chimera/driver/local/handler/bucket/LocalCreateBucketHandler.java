package win.ixuni.chimera.driver.local.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统创建 Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
public class LocalCreateBucketHandler implements OperationHandler<CreateBucketOperation, S3Bucket> {

    @Override
    public Mono<S3Bucket> handle(CreateBucketOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            var bucketPath = ctx.getBucketPath(bucketName);
            var bucketMetaPath = ctx.getBucketMetaPath(bucketName);

            // Idempotent: if bucket already exists, return existing bucket info
            if (Files.exists(bucketPath)) {
                BasicFileAttributes attrs = Files.readAttributes(bucketPath, BasicFileAttributes.class);
                return S3Bucket.builder()
                        .name(bucketName)
                        .creationDate(attrs.creationTime().toInstant())
                        .build();
            }

            // Create both data and meta bucket directories
            Files.createDirectories(bucketPath);
            Files.createDirectories(bucketMetaPath);

            return S3Bucket.builder()
                    .name(bucketName)
                    .creationDate(Instant.now())
                    .build();
        });
    }

    @Override
    public Class<CreateBucketOperation> getOperationType() {
        return CreateBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
