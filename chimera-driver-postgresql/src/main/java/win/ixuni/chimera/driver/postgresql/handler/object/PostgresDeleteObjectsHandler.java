package win.ixuni.chimera.driver.postgresql.handler.object;

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
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL DeleteObjects 批量删除处理器
 */
@Slf4j
public class PostgresDeleteObjectsHandler extends AbstractPostgresHandler<DeleteObjectsOperation, DeleteObjectsResult> {

        @Override
        protected Mono<DeleteObjectsResult> doHandle(DeleteObjectsOperation operation, PostgresDriverContext context) {
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

        private Mono<DeleteObjectsResult> deleteObjectsInternal(PostgresDriverContext context, String bucketName,
                        List<String> keys) {
                return Flux.fromIterable(keys)
                                .flatMap(key -> context.execute(new DeleteObjectOperation(bucketName, key))
                                                .thenReturn(DeleteObjectsResult.DeletedObject.builder()
                                                                .key(key)
                                                                .build())
                                                .onErrorResume(e -> {
                                                        log.warn("删除对象失败: bucket={}, key={}, error={}",
                                                                        bucketName, key, e.getMessage());
                                                        return Mono.just(DeleteObjectsResult.DeletedObject.builder()
                                                                        .key(key)
                                                                        .build());
                                                }))
                                .collectList()
                                .map(deleted -> DeleteObjectsResult.builder()
                                                .deleted(deleted)
                                                .build())
                                .doOnNext(result -> log.debug("DeleteObjects: bucket={}, 删除 {} 个对象",
                                                bucketName, result.getDeleted().size()));
        }

        @Override
        public Class<DeleteObjectsOperation> getOperationType() {
                return DeleteObjectsOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.WRITE);
        }
}
