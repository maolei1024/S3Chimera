package win.ixuni.chimera.driver.s3.handler.bucket;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理检查 Bucket 是否存在处理器
 */
public class S3BucketExistsHandler implements OperationHandler<BucketExistsOperation, Boolean> {

    @Override
    public Mono<Boolean> handle(BucketExistsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromFuture(() -> ctx.getS3Client().headBucket(
                HeadBucketRequest.builder()
                        .bucket(bucketName)
                        .build()))
                .map(response -> true)
                .onErrorResume(NoSuchBucketException.class, e -> Mono.just(false))
                .onErrorResume(S3Exception.class, e -> {
                    // 404 means bucket does not exist; other S3 exceptions should propagate
                    if (e.statusCode() == 404) {
                        return Mono.just(false);
                    }
                    return Mono.error(e);
                });
    }

    @Override
    public Class<BucketExistsOperation> getOperationType() {
        return BucketExistsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
