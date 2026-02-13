package win.ixuni.chimera.driver.postgresql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;

/**
 * PostgreSQL CopyObject 处理器
 */
@Slf4j
public class PostgresCopyObjectHandler extends AbstractPostgresHandler<CopyObjectOperation, S3Object> {

    @Override
    protected Mono<S3Object> doHandle(CopyObjectOperation operation, PostgresDriverContext context) {
        String srcBucket = operation.getSourceBucket();
        String srcKey = operation.getSourceKey();
        String destBucket = operation.getDestinationBucket();
        String destKey = operation.getDestinationKey();

        // 1. 获取源对象元数据 (包括 upload_version)
        String selectSrcSql = "SELECT size, etag, content_type, chunk_count, upload_version, user_metadata FROM t_object WHERE bucket_name = $1 AND object_key = $2";

        return context.executeQuery(context.getMetaConnectionPool(), selectSrcSql, srcBucket, srcKey).next()
                .map(row -> {
                    return new ObjectMetadata(row.get("size", Long.class), row.get("etag", String.class),
                            row.get("content_type", String.class), row.get("chunk_count", Integer.class),
                            row.get("upload_version", String.class), row.get("user_metadata", String.class));
                }).switchIfEmpty(Mono.error(new ObjectNotFoundException(srcBucket, srcKey))).flatMap(srcMeta -> {
                    ShardingRouter.ShardInfo srcShardInfo = context.getShardingRouter().getShardInfo(srcBucket, srcKey);
                    ShardingRouter.ShardInfo destShardInfo = context.getShardingRouter().getShardInfo(destBucket,
                            destKey);
                    ConnectionPool srcPool = context.getDataConnectionPool(srcBucket, srcKey);
                    ConnectionPool destPool = context.getDataConnectionPool(destBucket, destKey);

                    AtomicLong totalSize = new AtomicLong(0);
                    AtomicInteger chunkCount = new AtomicInteger(0);
                    MessageDigest fileDigest;
                    try {
                        fileDigest = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        return Mono.error(new RuntimeException("MD5 not available", e));
                    }

                    String srcVersion = srcMeta.uploadVersion == null ? "" : srcMeta.uploadVersion;
                    String newVersion = "v:" + UUID.randomUUID().toString();

                    // 2. 先删除目标位置的旧数据
                    return context.execute(new DeleteObjectOperation(destBucket, destKey)).then(Mono.defer(() -> {
                        // 3. 复制数据块 (带版本)
                        String srcChunkSql = "SELECT chunk_data, chunk_size, checksum FROM "
                                + srcShardInfo.getChunkTable()
                                + " WHERE bucket_name = $1 AND object_key = $2 AND upload_id = $3 ORDER BY chunk_index";

                        return context.executeQuery(srcPool, srcChunkSql, srcBucket, srcKey, srcVersion)
                                .flatMap(chunkRow -> {
                                    byte[] data = chunkRow.get("chunk_data", byte[].class);
                                    totalSize.addAndGet(data.length);
                                    int idx = chunkCount.getAndIncrement();
                                    synchronized (fileDigest) {
                                        fileDigest.update(data);
                                    }

                                    String insertSql = "INSERT INTO " + destShardInfo.getChunkTable()
                                            + " (bucket_name, object_key, upload_id, part_number, chunk_index, "
                                            + "chunk_data, chunk_size, checksum) VALUES ($1, $2, $3, 0, $4, $5, $6, $7)";

                                    return context.executeUpdate(destPool, insertSql, destBucket, destKey, newVersion,
                                            idx, data, data.length, chunkRow.get("checksum", String.class));
                                }).then();
                    })).then(Mono.defer(() -> {
                        String etag;
                        synchronized (fileDigest) {
                            etag = "\"" + bytesToHex(fileDigest.digest()) + "\"";
                        }
                        Instant now = Instant.now();

                        // Use PostgreSQL UPSERT (ON CONFLICT ... DO UPDATE)
                        String upsertSql = "INSERT INTO t_object "
                                + "(bucket_name, object_key, size, etag, content_type, last_modified, chunk_count, "
                                + "chunk_size, data_db_index, user_metadata, upload_version) "
                                + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11) "
                                + "ON CONFLICT (bucket_name, object_key) DO UPDATE SET "
                                + "size = $3, etag = $4, content_type = $5, last_modified = $6, chunk_count = $7, "
                                + "chunk_size = $8, data_db_index = $9, user_metadata = $10::jsonb, upload_version = $11";

                        return context
                                .executeUpdate(context.getMetaConnectionPool(), upsertSql, destBucket, destKey,
                                        totalSize.get(), etag,
                                        srcMeta.contentType != null ? srcMeta.contentType : "application/octet-stream",
                                        now, chunkCount.get(), context.getPostgresConfig().getChunkSize(),
                                        destShardInfo.getDataDbIndex(), srcMeta.userMetadata, newVersion)
                                .thenReturn(S3Object.builder().bucketName(destBucket).key(destKey).size(totalSize.get())
                                        .etag(etag).contentType(srcMeta.contentType).lastModified(now)
                                        .storageClass("STANDARD").build());
                    }));
                })
                .doOnSuccess(obj -> log.debug("CopyObject: {}:{} -> {}:{}", srcBucket, srcKey, destBucket, destKey));
    }

    private static class ObjectMetadata {
        final Long size;
        final String etag;
        final String contentType;
        final Integer chunkCount;
        final String uploadVersion;
        final String userMetadata;

        ObjectMetadata(Long size, String etag, String contentType, Integer chunkCount, String uploadVersion,
                String userMetadata) {
            this.size = size;
            this.etag = etag;
            this.contentType = contentType;
            this.chunkCount = chunkCount;
            this.uploadVersion = uploadVersion;
            this.userMetadata = userMetadata;
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Class<CopyObjectOperation> getOperationType() {
        return CopyObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.READ);
    }
}
