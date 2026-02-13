package win.ixuni.chimera.driver.postgresql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL DeleteObject 处理器
 */
@Slf4j
public class PostgresDeleteObjectHandler extends AbstractPostgresHandler<DeleteObjectOperation, Void> {

    @Override
    protected Mono<Void> doHandle(DeleteObjectOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 先检查 Bucket 是否存在
        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return deleteObjectInternal(context, bucketName, key);
                });
    }

    private Mono<Void> deleteObjectInternal(PostgresDriverContext context, String bucketName, String key) {
        ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);

        // 1. 删除数据块
        String deleteChunkSql = "DELETE FROM " + shardInfo.getChunkTable() +
                " WHERE bucket_name = $1 AND object_key = $2";

        return context.executeUpdate(context.getDataConnectionPool(bucketName, key),
                deleteChunkSql, bucketName, key)
                // 2. 删除对象元数据
                .then(context.executeUpdate(context.getMetaConnectionPool(),
                        "DELETE FROM t_object WHERE bucket_name = $1 AND object_key = $2",
                        bucketName, key))
                .doOnSuccess(count -> log.debug("DeleteObject: bucket={}, key={}", bucketName, key))
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
