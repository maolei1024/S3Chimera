package win.ixuni.chimera.driver.mysql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 批量删除对象处理器
 */
@Slf4j
public class MysqlDeleteObjectsHandler extends AbstractMysqlHandler<DeleteObjectsOperation, DeleteObjectsResult> {

        @Override
        protected Mono<DeleteObjectsResult> doHandle(DeleteObjectsOperation operation, MysqlDriverContext ctx) {
                DeleteObjectsRequest request = operation.getRequest();
                String bucket = request.getBucketName();

                // Use context.execute() to check if bucket exists
                return ctx.execute(new BucketExistsOperation(bucket))
                                .flatMap(exists -> {
                                        if (!exists) {
                                                return Mono.error(new BucketNotFoundException(bucket));
                                        }
                                        return deleteObjectsInternal(ctx, bucket, request.getKeys());
                                });
        }

        private Mono<DeleteObjectsResult> deleteObjectsInternal(MysqlDriverContext ctx, String bucket,
                        List<String> keys) {
                List<DeleteObjectsResult.DeletedObject> deleted = Collections.synchronizedList(new ArrayList<>());
                List<DeleteObjectsResult.DeleteError> errors = Collections.synchronizedList(new ArrayList<>());

                // 按 chunk 分表分组
                java.util.Map<String, List<String>> keysByChunkTable = new java.util.HashMap<>();
                for (String key : keys) {
                        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucket, key);
                        keysByChunkTable.computeIfAbsent(shardInfo.getChunkTable(), k -> new ArrayList<>()).add(key);
                }

                // 1. 批量删除 chunks（按分表分组）
                Mono<Void> deleteChunks = Flux.fromIterable(keysByChunkTable.entrySet())
                                .flatMap(entry -> {
                                        String chunkTable = entry.getKey();
                                        List<String> tableKeys = entry.getValue();
                                        String keyList = tableKeys.stream()
                                                        .map(k -> "'" + k.replace("'", "''") + "'")
                                                        .collect(java.util.stream.Collectors.joining(","));

                                        String sql = "DELETE FROM " + chunkTable +
                                                        " WHERE bucket_name = ? AND object_key IN (" + keyList + ")";

                                        // Determine which database connection pool to use (route by first key)
                                        return ctx.executeUpdate(ctx.getDataConnectionPool(bucket, tableKeys.get(0)),
                                                        sql, bucket)
                                                        .doOnSuccess(count -> log.debug("Deleted {} chunks from {}",
                                                                        count, chunkTable))
                                                        .onErrorResume(e -> {
                                                                log.warn("Failed to delete chunks from {}: {}",
                                                                                chunkTable, e.getMessage());
                                                                return Mono.just(0L);
                                                        });
                                })
                                .then();

                // 2. 批量删除元数据（元数据表不分片）
                String allKeysList = keys.stream()
                                .map(k -> "'" + k.replace("'", "''") + "'")
                                .collect(java.util.stream.Collectors.joining(","));

                // Get shard info for any key (to get table name)
                ShardingRouter.ShardInfo sampleShardInfo = ctx.getShardingRouter().getShardInfo(bucket, keys.get(0));

                String deleteMetadataSql = "DELETE FROM " + sampleShardInfo.getObjectMetadataTable() +
                                " WHERE bucket_name = ? AND object_key IN (" + allKeysList + ")";
                String deleteObjectSql = "DELETE FROM " + sampleShardInfo.getObjectTable() +
                                " WHERE bucket_name = ? AND object_key IN (" + allKeysList + ")";

                Mono<Void> deleteMetadata = ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteMetadataSql, bucket)
                                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteObjectSql, bucket))
                                .then();

                // 3. 执行删除并构建结果
                return deleteChunks
                                .then(deleteMetadata)
                                .then(Mono.fromSupplier(() -> {
                                        // All keys marked as deleted (S3 semantics: deleting non-existent objects also returns success)
                                        for (String key : keys) {
                                                deleted.add(DeleteObjectsResult.DeletedObject.builder()
                                                                .key(key)
                                                                .build());
                                        }
                                        return DeleteObjectsResult.builder()
                                                        .deleted(new ArrayList<>(deleted))
                                                        .errors(errors.isEmpty() ? null : new ArrayList<>(errors))
                                                        .build();
                                }))
                                .onErrorResume(e -> {
                                        log.error("Batch delete failed: {}", e.getMessage());
                                        // 回退到逐个删除模式
                                        return deleteObjectsOneByOne(ctx, bucket, keys, deleted, errors);
                                });
        }

        /**
         * Fallback: delete one by one (used when batch delete fails)
         */
        private Mono<DeleteObjectsResult> deleteObjectsOneByOne(MysqlDriverContext ctx, String bucket,
                        List<String> keys, List<DeleteObjectsResult.DeletedObject> deleted,
                        List<DeleteObjectsResult.DeleteError> errors) {
                return Flux.fromIterable(keys)
                                .flatMap(key -> {
                                        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter()
                                                        .getShardInfo(bucket, key);

                                        String deleteChunksSql = "DELETE FROM " + shardInfo.getChunkTable() +
                                                        " WHERE bucket_name = ? AND object_key = ?";
                                        String deleteMetadataSql = "DELETE FROM " + shardInfo.getObjectMetadataTable() +
                                                        " WHERE bucket_name = ? AND object_key = ?";
                                        String deleteObjectSql = "DELETE FROM " + shardInfo.getObjectTable() +
                                                        " WHERE bucket_name = ? AND object_key = ?";

                                        return ctx.executeUpdate(ctx.getDataConnectionPool(bucket, key),
                                                        deleteChunksSql, bucket, key)
                                                        .then(ctx.executeUpdate(ctx.getMetaConnectionPool(),
                                                                        deleteMetadataSql, bucket, key))
                                                        .then(ctx.executeUpdate(ctx.getMetaConnectionPool(),
                                                                        deleteObjectSql, bucket, key))
                                                        .then(Mono.fromRunnable(() -> deleted
                                                                        .add(DeleteObjectsResult.DeletedObject.builder()
                                                                                        .key(key)
                                                                                        .build())))
                                                        .onErrorResume(e -> {
                                                                errors.add(DeleteObjectsResult.DeleteError.builder()
                                                                                .key(key)
                                                                                .code("InternalError")
                                                                                .message(e.getMessage())
                                                                                .build());
                                                                return Mono.empty();
                                                        });
                                }, 5)
                                .then(Mono.fromSupplier(() -> DeleteObjectsResult.builder()
                                                .deleted(new ArrayList<>(deleted))
                                                .errors(errors.isEmpty() ? null : new ArrayList<>(errors))
                                                .build()));
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
