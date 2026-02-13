package win.ixuni.chimera.driver.mysql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 上传对象处理器
 */
@Slf4j
public class MysqlPutObjectHandler extends AbstractMysqlHandler<PutObjectOperation, S3Object> {

    @Override
    protected Mono<S3Object> doHandle(PutObjectOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // Use context.execute() to invoke other operations instead of directly instantiating handlers
        return ctx.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return collectAndStoreObject(ctx, operation);
                });
    }

    private Mono<S3Object> collectAndStoreObject(MysqlDriverContext ctx, PutObjectOperation op) {
        String bucketName = op.getBucketName();
        String key = op.getKey();
        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);
        ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

        AtomicLong totalSize = new AtomicLong(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        final int chunkSize = ctx.getMysqlConfig().getChunkSize();
        final int writeConcurrency = ctx.getMysqlConfig().getWriteConcurrency();

        final MessageDigest fileDigest;
        try {
            fileDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return Mono.error(new RuntimeException("MD5 not available", e));
        }

        // First delete old data using ctx.execute() to call DeleteObjectOperation
        return ctx.execute(new DeleteObjectOperation(bucketName, key))
                .then(Mono.defer(() -> {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream(chunkSize);

                    return op.getContent()
                            .concatMap(byteBuffer -> {
                                return Flux.defer(() -> {
                                    List<byte[]> chunks = new ArrayList<>();
                                    while (byteBuffer.hasRemaining()) {
                                        int spaceLeft = chunkSize - buffer.size();
                                        int toWrite = Math.min(spaceLeft, byteBuffer.remaining());
                                        byte[] temp = new byte[toWrite];
                                        byteBuffer.get(temp);
                                        buffer.write(temp, 0, toWrite);
                                        if (buffer.size() >= chunkSize) {
                                            chunks.add(buffer.toByteArray());
                                            buffer.reset();
                                        }
                                    }
                                    return Flux.fromIterable(chunks);
                                });
                            })
                            .concatWith(Mono.defer(() -> {
                                if (buffer.size() > 0) {
                                    return Mono.just(buffer.toByteArray());
                                }
                                return Mono.empty();
                            }))
                            .map(chunkData -> {
                                totalSize.addAndGet(chunkData.length);
                                int idx = chunkIndex.getAndIncrement();
                                synchronized (fileDigest) {
                                    fileDigest.update(chunkData);
                                }
                                String chunkChecksum = bytesToHex(calculateMd5(chunkData));
                                return new ChunkInfo(idx, chunkData, chunkChecksum);
                            })
                            .limitRate(writeConcurrency + 1)
                            .flatMap(chunkInfo -> insertChunk(ctx, dataPool, shardInfo.getChunkTable(),
                                    bucketName, key, chunkInfo.index, chunkInfo.data, chunkInfo.checksum)
                                    .doFinally(s -> chunkInfo.clearData()),
                                    writeConcurrency)
                            .then();
                }))
                .then(Mono.defer(() -> {
                    String etag;
                    synchronized (fileDigest) {
                        etag = "\"" + bytesToHex(fileDigest.digest()) + "\"";
                    }
                    Instant now = Instant.now();

                    // Convert metadata to JSON string for t_object.user_metadata column
                    String userMetadataJson = convertMetadataToJson(op.getMetadata());

                    return insertObjectMetadata(ctx, bucketName, key, totalSize.get(), etag,
                            op.getContentType(), chunkIndex.get(), userMetadataJson, shardInfo)
                            .then(saveUserMetadata(ctx, bucketName, key, op.getMetadata(), shardInfo))
                            .thenReturn(S3Object.builder()
                                    .bucketName(bucketName)
                                    .key(key)
                                    .size(totalSize.get())
                                    .etag(etag)
                                    .contentType(op.getContentType() != null ? op.getContentType()
                                            : "application/octet-stream")
                                    .lastModified(now)
                                    .storageClass("STANDARD")
                                    .build());
                }));
    }

    private Mono<Void> insertChunk(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
            String bucketName, String key, int chunkIndex, byte[] data, String checksum) {
        // Use INSERT ... ON DUPLICATE KEY UPDATE to avoid deadlocks under concurrency
        // Unlike REPLACE INTO, ON DUPLICATE KEY UPDATE is a single-step atomic operation that avoids deadlocks
        String sql = "INSERT INTO " + tableName +
                " (bucket_name, object_key, upload_id, part_number, chunk_index, chunk_data, chunk_size, checksum) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE chunk_data = VALUES(chunk_data), chunk_size = VALUES(chunk_size), checksum = VALUES(checksum)";

        return Mono.usingWhen(
                Mono.from(pool.create()),
                connection -> {
                    var stmt = connection.createStatement(sql)
                            .bind(0, bucketName)
                            .bind(1, key)
                            .bindNull(2, String.class)
                            .bind(3, 0)
                            .bind(4, chunkIndex)
                            .bind(5, data)
                            .bind(6, data.length)
                            .bind(7, checksum);
                    return Mono.from(stmt.execute())
                            .flatMap(result -> Mono.from(result.getRowsUpdated()))
                            .then();
                },
                connection -> Mono.from(connection.close()));
    }

    private Mono<Void> insertObjectMetadata(MysqlDriverContext ctx, String bucketName, String key,
            long size, String etag, String contentType, int chunkCount, String userMetadataJson,
            ShardingRouter.ShardInfo shardInfo) {
        // Use INSERT ... ON DUPLICATE KEY UPDATE to avoid deadlocks under concurrency
        // Unlike REPLACE INTO, ON DUPLICATE KEY UPDATE is a single-step atomic operation that avoids deadlocks
        String sql = "INSERT INTO " + shardInfo.getObjectTable() +
                " (bucket_name, object_key, size, etag, content_type, last_modified, chunk_count, chunk_size, user_metadata, data_db_index) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE size = VALUES(size), etag = VALUES(etag), content_type = VALUES(content_type), "
                +
                "last_modified = VALUES(last_modified), chunk_count = VALUES(chunk_count), chunk_size = VALUES(chunk_size), "
                +
                "user_metadata = VALUES(user_metadata), data_db_index = VALUES(data_db_index)";

        return Mono.usingWhen(
                Mono.from(ctx.getMetaConnectionPool().create()),
                connection -> {
                    var stmt = connection.createStatement(sql)
                            .bind(0, bucketName)
                            .bind(1, key)
                            .bind(2, size)
                            .bind(3, etag)
                            .bind(4, contentType != null ? contentType : "application/octet-stream")
                            .bind(5, LocalDateTime.now())
                            .bind(6, chunkCount)
                            .bind(7, ctx.getMysqlConfig().getChunkSize());

                    if (userMetadataJson != null) {
                        stmt = stmt.bind(8, userMetadataJson);
                    } else {
                        stmt = stmt.bindNull(8, String.class);
                    }

                    return Mono.from(stmt.bind(9, shardInfo.getDataDbIndex()).execute())
                            .flatMap(result -> Mono.from(result.getRowsUpdated()))
                            .then();
                },
                connection -> Mono.from(connection.close()));
    }

    private String convertMetadataToJson(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        // 简单 JSON 序列化
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (!first)
                sb.append(",");
            first = false;
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
            sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String s) {
        if (s == null)
            return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private Mono<Void> saveUserMetadata(MysqlDriverContext ctx, String bucketName, String key,
            Map<String, String> metadata, ShardingRouter.ShardInfo shardInfo) {
        if (metadata == null || metadata.isEmpty()) {
            return Mono.empty();
        }

        return Flux.fromIterable(metadata.entrySet())
                .flatMap(entry -> {
                    // Use INSERT ... ON DUPLICATE KEY UPDATE to avoid deadlocks under concurrency
                    String sql = "INSERT INTO " + shardInfo.getObjectMetadataTable() +
                            " (bucket_name, object_key, meta_key, meta_value) VALUES (?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)";
                    return ctx.executeUpdate(ctx.getMetaConnectionPool(), sql,
                            bucketName, key, entry.getKey(), entry.getValue());
                })
                .then();
    }

    private byte[] calculateMd5(byte[] data) {
        try {
            return MessageDigest.getInstance("MD5").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static class ChunkInfo {
        final int index;
        byte[] data;
        final String checksum;

        ChunkInfo(int index, byte[] data, String checksum) {
            this.index = index;
            this.data = data;
            this.checksum = checksum;
        }

        void clearData() {
            this.data = null;
        }
    }

    @Override
    public Class<PutObjectOperation> getOperationType() {
        return PutObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
