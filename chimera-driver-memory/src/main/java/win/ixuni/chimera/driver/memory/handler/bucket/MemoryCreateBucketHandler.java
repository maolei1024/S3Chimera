package win.ixuni.chimera.driver.memory.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.time.Instant;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 创建 Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
public class MemoryCreateBucketHandler implements OperationHandler<CreateBucketOperation, S3Bucket> {

    @Override
    public Mono<S3Bucket> handle(CreateBucketOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();

        // Idempotent: if bucket already exists, return existing bucket info directly
        MemoryDriverContext.BucketInfo existingBucket = ctx.getBuckets().get(bucketName);
        if (existingBucket != null) {
            return Mono.just(S3Bucket.builder()
                    .name(existingBucket.getName())
                    .creationDate(existingBucket.getCreationDate())
                    .build());
        }

        Instant now = Instant.now();
        ctx.getBuckets().put(bucketName, MemoryDriverContext.BucketInfo.builder()
                .name(bucketName)
                .creationDate(now)
                .build());

        return Mono.just(S3Bucket.builder()
                .name(bucketName)
                .creationDate(now)
                .build());
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
