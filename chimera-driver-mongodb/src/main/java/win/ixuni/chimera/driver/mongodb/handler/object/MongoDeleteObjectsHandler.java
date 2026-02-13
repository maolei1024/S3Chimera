package win.ixuni.chimera.driver.mongodb.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB DeleteObjects 批量删除处理器
 */
@Slf4j
public class MongoDeleteObjectsHandler extends AbstractMongoHandler<DeleteObjectsOperation, DeleteObjectsResult> {

        @Override
        protected Mono<DeleteObjectsResult> doHandle(DeleteObjectsOperation operation, MongoDriverContext context) {
                DeleteObjectsRequest request = operation.getRequest();
                String bucketName = request.getBucketName();
                List<String> keys = request.getKeys();

                // 先检查 Bucket 是否存在
                return context.execute(new BucketExistsOperation(bucketName))
                                .flatMap(exists -> {
                                        if (!exists) {
                                                return Mono.error(new BucketNotFoundException(bucketName));
                                        }
                                        return deleteObjectsInternal(context, bucketName, keys);
                                });
        }

        private Mono<DeleteObjectsResult> deleteObjectsInternal(MongoDriverContext context, String bucketName,
                        List<String> keys) {
                return Flux.fromIterable(keys)
                                .flatMap(key -> context.execute(new DeleteObjectOperation(bucketName, key))
                                                .thenReturn((Object) DeleteObjectsResult.DeletedObject.builder()
                                                                .key(key)
                                                                .build())
                                                .onErrorResume(e -> {
                                                        log.warn("删除对象失败: bucket={}, key={}, error={}",
                                                                        bucketName, key, e.getMessage());
                                                        return Mono.just((Object) DeleteObjectsResult.DeleteError
                                                                        .builder()
                                                                        .key(key)
                                                                        .code("InternalError")
                                                                        .message(e.getMessage())
                                                                        .build());
                                                }))
                                .collectList()
                                .map(results -> {
                                        var deleted = results.stream()
                                                        .filter(r -> r instanceof DeleteObjectsResult.DeletedObject)
                                                        .map(r -> (DeleteObjectsResult.DeletedObject) r)
                                                        .toList();
                                        var errors = results.stream()
                                                        .filter(r -> r instanceof DeleteObjectsResult.DeleteError)
                                                        .map(r -> (DeleteObjectsResult.DeleteError) r)
                                                        .toList();
                                        return DeleteObjectsResult.builder()
                                                        .deleted(deleted)
                                                        .errors(errors)
                                                        .build();
                                })
                                .doOnNext(result -> log.debug("DeleteObjects: bucket={}, 删除 {} 个对象, {} 个失败",
                                                bucketName, result.getDeleted().size(),
                                                result.getErrors() != null ? result.getErrors().size() : 0));
        }

        @Override
        public Class<DeleteObjectsOperation> getOperationType() {
                return DeleteObjectsOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.WRITE, Capability.BATCH_DELETE);
        }
}
