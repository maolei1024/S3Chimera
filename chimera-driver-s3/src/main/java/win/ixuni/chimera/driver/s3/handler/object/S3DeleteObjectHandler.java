package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理删除对象处理器
 */
public class S3DeleteObjectHandler implements OperationHandler<DeleteObjectOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteObjectOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;

        return Mono.fromFuture(() -> ctx.getS3Client().deleteObject(
                DeleteObjectRequest.builder()
                        .bucket(operation.getBucketName())
                        .key(operation.getKey())
                        .build()))
                .then();
    }

    @Override
    public Class<DeleteObjectOperation> getOperationType() {
        return DeleteObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
