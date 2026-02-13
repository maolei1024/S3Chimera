package win.ixuni.chimera.driver.mysql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 删除对象处理器
 */
@Slf4j
public class MysqlDeleteObjectHandler extends AbstractMysqlHandler<DeleteObjectOperation, Void> {

        @Override
        protected Mono<Void> doHandle(DeleteObjectOperation operation, MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();

                // Use context.execute() to check if bucket exists
                return ctx.execute(new BucketExistsOperation(bucketName))
                                .flatMap(exists -> {
                                        if (!exists) {
                                                return Mono.error(new BucketNotFoundException(bucketName));
                                        }
                                        return deleteObjectInternal(ctx, bucketName, key);
                                });
        }

        private Mono<Void> deleteObjectInternal(MysqlDriverContext ctx, String bucketName, String key) {
                ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);

                // 删除 chunks
                String deleteChunksSql = "DELETE FROM " + shardInfo.getChunkTable() +
                                " WHERE bucket_name = ? AND object_key = ?";

                // Delete user metadata
                String deleteMetadataSql = "DELETE FROM " + shardInfo.getObjectMetadataTable() +
                                " WHERE bucket_name = ? AND object_key = ?";

                // 删除对象记录
                String deleteObjectSql = "DELETE FROM " + shardInfo.getObjectTable() +
                                " WHERE bucket_name = ? AND object_key = ?";

                return ctx.executeUpdate(ctx.getDataConnectionPool(bucketName, key), deleteChunksSql, bucketName, key)
                                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteMetadataSql, bucketName,
                                                key))
                                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteObjectSql, bucketName, key))
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
