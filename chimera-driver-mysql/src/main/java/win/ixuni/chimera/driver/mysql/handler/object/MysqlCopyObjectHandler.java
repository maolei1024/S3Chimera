package win.ixuni.chimera.driver.mysql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL CopyObject 处理器
 */
@Slf4j
public class MysqlCopyObjectHandler extends AbstractMysqlHandler<CopyObjectOperation, S3Object> {

        @Override
        protected Mono<S3Object> doHandle(CopyObjectOperation operation, MysqlDriverContext context) {
                String srcBucket = operation.getSourceBucket();
                String srcKey = operation.getSourceKey();
                String destBucket = operation.getDestinationBucket();
                String destKey = operation.getDestinationKey();

                log.debug("copyObject: src={}/{}, dest={}/{}", srcBucket, srcKey, destBucket, destKey);

                // 1. 获取源对象元数据
                // Use Mono.usingWhen to ensure Row is fully consumed before connection closes
                // 这避免了 R2DBC MySQL 驱动在并发场景下的 refCnt:0 问题
                String selectSrcSql = "SELECT size, etag, content_type, chunk_count, user_metadata FROM t_object WHERE bucket_name = ? AND object_key = ?";

                return Mono.usingWhen(
                                Mono.from(context.getMetaConnectionPool().create()),
                                connection -> {
                                        var stmt = connection.createStatement(selectSrcSql);
                                        stmt.bind(0, srcBucket);
                                        stmt.bind(1, srcKey);
                                        return Mono.from(stmt.execute())
                                                        .flatMapMany(result -> reactor.core.publisher.Flux
                                                                        .from(result.map((row, meta) -> {
                                                                                // Extract all data immediately inside result.map callback, ensuring Row's
                                                                                // ByteBuf is still valid
                                                                                return new ObjectMetadata(
                                                                                                row.get("size", Long.class),
                                                                                                row.get("etag", String.class),
                                                                                                row.get("content_type",
                                                                                                                String.class),
                                                                                                row.get("chunk_count",
                                                                                                                Integer.class),
                                                                                                row.get("user_metadata",
                                                                                                                String.class));
                                                                        })))
                                                        .next();
                                },
                                connection -> Mono.from(connection.close()))
                                .switchIfEmpty(Mono.error(new ObjectNotFoundException(srcBucket, srcKey)))
                                .flatMap(srcMeta -> {
                                        // 分片信息
                                        ShardingRouter.ShardInfo srcShardInfo = context.getShardingRouter()
                                                        .getShardInfo(srcBucket, srcKey);
                                        ShardingRouter.ShardInfo destShardInfo = context.getShardingRouter()
                                                        .getShardInfo(destBucket,
                                                                        destKey);

                                        // 获取数据连接池 (Chunks 所在)
                                        ConnectionPool destPool = context.getDataConnectionPool(destBucket, destKey);

                                        // 2. 先删除目标位置的旧数据
                                        return context.execute(new DeleteObjectOperation(destBucket, destKey))
                                                        .then(Mono.defer(() -> {
                                                                // 3. 复制数据块
                                                                // 支持跨分片复制：从源分片读取 chunks，写入目标分片
                                                                ConnectionPool srcPool = context.getDataConnectionPool(
                                                                                srcBucket, srcKey);

                                                                String selectChunkSql = "SELECT part_number, chunk_index, chunk_data, chunk_size, checksum FROM "
                                                                                + srcShardInfo.getChunkTable()
                                                                                + " WHERE bucket_name = ? AND object_key = ? AND upload_id IS NULL ORDER BY part_number, chunk_index";

                                                                // 读取源 chunks 并写入目标
                                                                return context.executeQueryMapped(srcPool,
                                                                                selectChunkSql,
                                                                                row -> new ChunkData(
                                                                                                row.get("chunk_index",
                                                                                                                Integer.class),
                                                                                                row.get("chunk_data",
                                                                                                                byte[].class),
                                                                                                row.get("chunk_size",
                                                                                                                Integer.class),
                                                                                                row.get("checksum",
                                                                                                                String.class)),
                                                                                srcBucket, srcKey)
                                                                                .flatMap(chunk -> insertChunk(context,
                                                                                                destPool,
                                                                                                destShardInfo.getChunkTable(),
                                                                                                destBucket, destKey,
                                                                                                chunk), 4) // 并发度 4
                                                                                .then();
                                                        }))
                                                        .then(Mono.defer(() -> {
                                                                // 4. Create target object metadata (using manual binding to handle NULL values)
                                                                String insertSql = "INSERT INTO t_object " +
                                                                                "(bucket_name, object_key, size, etag, content_type, last_modified, chunk_count, "
                                                                                +
                                                                                "chunk_size, user_metadata, data_db_index) "
                                                                                +
                                                                                "VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?)";

                                                                int chunkS = 1048576;
                                                                try {
                                                                        chunkS = context.getMysqlConfig()
                                                                                        .getChunkSize();
                                                                } catch (Exception e) {
                                                                        /* ignore */ }

                                                                final int finalChunkS = chunkS;
                                                                final Long size = srcMeta.size != null ? srcMeta.size
                                                                                : 0L;
                                                                final String etag = srcMeta.etag != null ? srcMeta.etag
                                                                                : "d41d8cd98f00b204e9800998ecf8427e";
                                                                final String contentType = srcMeta.contentType != null
                                                                                ? srcMeta.contentType
                                                                                : "application/octet-stream";
                                                                final Integer chunkCount = srcMeta.chunkCount != null
                                                                                ? srcMeta.chunkCount
                                                                                : 0;

                                                                return Mono.usingWhen(
                                                                                Mono.from(context
                                                                                                .getMetaConnectionPool()
                                                                                                .create()),
                                                                                connection -> {
                                                                                        var stmt = connection
                                                                                                        .createStatement(
                                                                                                                        insertSql)
                                                                                                        .bind(0, destBucket)
                                                                                                        .bind(1, destKey)
                                                                                                        .bind(2, size)
                                                                                                        .bind(3, etag)
                                                                                                        .bind(4, contentType)
                                                                                                        .bind(5, chunkCount)
                                                                                                        .bind(6, finalChunkS);

                                                                                        if (srcMeta.userMetadata != null) {
                                                                                                stmt = stmt.bind(7,
                                                                                                                srcMeta.userMetadata);
                                                                                        } else {
                                                                                                stmt = stmt.bindNull(7,
                                                                                                                String.class);
                                                                                        }

                                                                                        return Mono.from(stmt.bind(8,
                                                                                                        destShardInfo.getDataDbIndex())
                                                                                                        .execute())
                                                                                                        .flatMap(result -> Mono
                                                                                                                        .from(result.getRowsUpdated()))
                                                                                                        .then();
                                                                                },
                                                                                connection -> Mono.from(
                                                                                                connection.close()))
                                                                                .thenReturn(
                                                                                                S3Object.builder()
                                                                                                                .bucketName(destBucket)
                                                                                                                .key(destKey)
                                                                                                                .size(size)
                                                                                                                .etag(etag)
                                                                                                                .contentType(contentType)
                                                                                                                .lastModified(java.time.Instant
                                                                                                                                .now())
                                                                                                                .storageClass("STANDARD")
                                                                                                                .build());
                                                        }));
                                });
        }

        private static class ObjectMetadata {
                final Long size;
                final String etag;
                final String contentType;
                final Integer chunkCount;
                final String userMetadata;

                ObjectMetadata(Long size, String etag, String contentType, Integer chunkCount, String userMetadata) {
                        this.size = size;
                        this.etag = etag;
                        this.contentType = contentType;
                        this.chunkCount = chunkCount;
                        this.userMetadata = userMetadata;
                }
        }

        private record ChunkData(Integer chunkIndex, byte[] chunkData, Integer chunkSize, String checksum) {
        }

        private Mono<Void> insertChunk(MysqlDriverContext context, ConnectionPool pool, String tableName,
                        String bucketName, String key, ChunkData chunk) {
                String sql = "INSERT INTO " + tableName +
                                " (bucket_name, object_key, upload_id, part_number, chunk_index, chunk_data, chunk_size, checksum) "
                                +
                                "VALUES (?, ?, NULL, 0, ?, ?, ?, ?)";

                return Mono.usingWhen(
                                Mono.from(pool.create()),
                                connection -> {
                                        var stmt = connection.createStatement(sql)
                                                        .bind(0, bucketName)
                                                        .bind(1, key)
                                                        .bind(2, chunk.chunkIndex())
                                                        .bind(3, chunk.chunkData())
                                                        .bind(4, chunk.chunkSize())
                                                        .bind(5, chunk.checksum() != null ? chunk.checksum() : "");
                                        return Mono.from(stmt.execute())
                                                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                                                        .then();
                                },
                                connection -> Mono.from(connection.close()));
        }

        @Override
        public Class<CopyObjectOperation> getOperationType() {
                return CopyObjectOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.COPY);
        }
}
