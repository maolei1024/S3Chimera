package win.ixuni.chimera.driver.mysql.handler.multipart;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 取消分片上传处理器
 */
@Slf4j
public class MysqlAbortMultipartUploadHandler extends AbstractMysqlHandler<AbortMultipartUploadOperation, Void> {

    @Override
    protected Mono<Void> doHandle(AbortMultipartUploadOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);
        ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

        // 删除 chunks
        String deleteChunksSql = "DELETE FROM " + shardInfo.getChunkTable() +
                " WHERE bucket_name = ? AND object_key = ? AND upload_id = ?";

        return ctx.executeUpdate(dataPool, deleteChunksSql, bucketName, key, uploadId)
                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(),
                        "DELETE FROM t_multipart_part WHERE upload_id = ?", uploadId))
                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(),
                        "DELETE FROM t_multipart_upload WHERE upload_id = ?", uploadId))
                .then();
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
