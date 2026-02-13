package win.ixuni.chimera.driver.postgresql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL 取消分片上传处理器
 */
@Slf4j
public class PostgresAbortMultipartUploadHandler extends AbstractPostgresHandler<AbortMultipartUploadOperation, Void> {

    @Override
    protected Mono<Void> doHandle(AbortMultipartUploadOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);

        // 1. Delete all chunks
        String deleteChunkSql = "DELETE FROM " + shardInfo.getChunkTable() + " WHERE upload_id = $1";

        return context.executeUpdate(context.getDataConnectionPool(bucketName, key), deleteChunkSql, uploadId)
                // 2. Delete all parts
                .then(context.executeUpdate(context.getMetaConnectionPool(),
                        "DELETE FROM t_multipart_part WHERE upload_id = $1", uploadId))
                // 3. 删除上传会话
                .then(context.executeUpdate(context.getMetaConnectionPool(),
                        "DELETE FROM t_multipart_upload WHERE upload_id = $1", uploadId))
                .doOnSuccess(count -> log.debug("AbortMultipartUpload: uploadId={}", uploadId))
                .then();
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
    }
}
