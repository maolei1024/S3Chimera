package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理批量删除对象处理器
 */
public class S3DeleteObjectsHandler implements OperationHandler<DeleteObjectsOperation, DeleteObjectsResult> {

    @Override
    public Mono<DeleteObjectsResult> handle(DeleteObjectsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        var request = operation.getRequest();

        var objectsToDelete = request.getKeys().stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .toList();

        var s3Request = DeleteObjectsRequest.builder()
                .bucket(request.getBucketName())
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        return Mono.fromFuture(() -> ctx.getS3Client().deleteObjects(s3Request))
                .map(response -> DeleteObjectsResult.builder()
                        .deleted(response.deleted().stream()
                                .map(d -> DeleteObjectsResult.DeletedObject.builder()
                                        .key(d.key())
                                        .build())
                                .toList())
                        .errors(response.errors().stream()
                                .map(e -> DeleteObjectsResult.DeleteError.builder()
                                        .key(e.key())
                                        .code(e.code())
                                        .message(e.message())
                                        .build())
                                .toList())
                        .build());
    }

    @Override
    public Class<DeleteObjectsOperation> getOperationType() {
        return DeleteObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.BATCH_DELETE);
    }
}
