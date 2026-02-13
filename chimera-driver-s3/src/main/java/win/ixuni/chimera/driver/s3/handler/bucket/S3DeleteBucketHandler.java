package win.ixuni.chimera.driver.s3.handler.bucket;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理删除 Bucket 处理器
 */
public class S3DeleteBucketHandler implements OperationHandler<DeleteBucketOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteBucketOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromFuture(() -> ctx.getS3Client().deleteBucket(
                DeleteBucketRequest.builder()
                        .bucket(bucketName)
                        .build()))
                .then();
    }

    @Override
    public Class<DeleteBucketOperation> getOperationType() {
        return DeleteBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
