package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理复制对象处理器
 */
public class S3CopyObjectHandler implements OperationHandler<CopyObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CopyObjectOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;

        return Mono.fromFuture(() -> ctx.getS3Client().copyObject(
                CopyObjectRequest.builder()
                        .sourceBucket(operation.getSourceBucket())
                        .sourceKey(operation.getSourceKey())
                        .destinationBucket(operation.getDestinationBucket())
                        .destinationKey(operation.getDestinationKey())
                        .build()))
                .map(response -> S3Object.builder()
                        .bucketName(operation.getDestinationBucket())
                        .key(operation.getDestinationKey())
                        .etag(response.copyObjectResult().eTag())
                        .lastModified(response.copyObjectResult().lastModified())
                        .build());
    }

    @Override
    public Class<CopyObjectOperation> getOperationType() {
        return CopyObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.COPY);
    }
}
