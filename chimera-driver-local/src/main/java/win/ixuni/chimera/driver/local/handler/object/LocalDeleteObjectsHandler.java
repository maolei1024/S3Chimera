package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * 本地文件系统批量删除对象处理器
 */
public class LocalDeleteObjectsHandler implements OperationHandler<DeleteObjectsOperation, DeleteObjectsResult> {

        @Override
        public Mono<DeleteObjectsResult> handle(DeleteObjectsOperation operation, DriverContext context) {
                LocalDriverContext ctx = (LocalDriverContext) context;
                DeleteObjectsRequest request = operation.getRequest();

                // Check bucket exists first
                return Mono.fromCallable(() -> {
                        var bucketPath = ctx.getBucketPath(request.getBucketName());
                        if (!java.nio.file.Files.exists(bucketPath)) {
                                throw new BucketNotFoundException(request.getBucketName());
                        }
                        return true;
                }).flatMapMany(ignored -> Flux.fromIterable(request.getKeys()))
                                .flatMap(key -> {
                                        DeleteObjectOperation deleteOp = new DeleteObjectOperation(
                                                        request.getBucketName(), key);
                                        return ctx.getHandlerRegistry().execute(deleteOp, context)
                                                        .thenReturn(DeleteObjectsResult.DeletedObject.builder()
                                                                        .key(key)
                                                                        .build())
                                                        .onErrorResume(e -> Mono.just(
                                                                        DeleteObjectsResult.DeletedObject.builder()
                                                                                        .key(key)
                                                                                        .build()));
                                })
                                .collectList()
                                .map(deleted -> DeleteObjectsResult.builder()
                                                .deleted(deleted)
                                                .errors(List.of())
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
