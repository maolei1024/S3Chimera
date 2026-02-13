package win.ixuni.chimera.driver.s3.handler.bucket;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理创建 Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
public class S3CreateBucketHandler implements OperationHandler<CreateBucketOperation, S3Bucket> {

    @Override
    public Mono<S3Bucket> handle(CreateBucketOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromFuture(() -> ctx.getS3Client().createBucket(
                CreateBucketRequest.builder()
                        .bucket(bucketName)
                        .build()))
                .map(response -> S3Bucket.builder()
                        .name(bucketName)
                        .creationDate(Instant.now())
                        .build())
                .onErrorResume(e -> {
                    // Idempotent: if bucket already exists (409 Conflict), return existing bucket info
                    if (e instanceof S3Exception s3Ex && s3Ex.statusCode() == 409) {
                        // Call headBucket to confirm bucket exists and get info
                        return Mono.fromFuture(() -> ctx.getS3Client().headBucket(
                                HeadBucketRequest.builder()
                                        .bucket(bucketName)
                                        .build()))
                                .map(headResponse -> S3Bucket.builder()
                                        .name(bucketName)
                                        .creationDate(Instant.now()) // S3 headBucket 不返回创建时间
                                        .build());
                    }
                    return Mono.error(e);
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
