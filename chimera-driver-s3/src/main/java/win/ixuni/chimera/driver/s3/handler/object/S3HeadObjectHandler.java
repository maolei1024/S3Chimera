package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理获取对象元数据处理器
 */
public class S3HeadObjectHandler implements OperationHandler<HeadObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(HeadObjectOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromFuture(() -> ctx.getS3Client().headObject(
                HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build()))
                .map(response -> S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size(response.contentLength())
                        .etag(response.eTag())
                        .lastModified(response.lastModified())
                        .contentType(response.contentType())
                        .userMetadata(response.metadata())
                        .build());
    }

    @Override
    public Class<HeadObjectOperation> getOperationType() {
        return HeadObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
