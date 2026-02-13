package win.ixuni.chimera.driver.postgresql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL PutObject 处理器
 * <p>
 * Uses versioning strategy to handle concurrent writes:
 * 1. Assign a unique uploadVersion per upload (stored in the upload_id field)
 * 2. 先插入新版本的 chunks（不删除旧数据）
 * 3. 原子更新 object 元数据指向新版本
 * 4. Asynchronously clean up old version chunks
 * <p>
 * Note: uses the upload_id field to store the version number since it is part of the unique index,
 * This way different versions of chunks can coexist without unique index conflicts.
 */
@Slf4j
public class PostgresPutObjectHandler extends AbstractPostgresHandler<PutObjectOperation, S3Object> {

    // Version prefix for regular PUT operations, to distinguish from multipart uploads
    private static final String VERSION_PREFIX = "v:";

    @Override
    protected Mono<S3Object> doHandle(PutObjectOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return collectAndStoreObject(context, operation);
                });
    }

    private Mono<S3Object> collectAndStoreObject(PostgresDriverContext context, PutObjectOperation op) {
        String bucketName = op.getBucketName();
        String key = op.getKey();
        // Generate unique version number for this upload (stored in upload_id field)
        String uploadVersion = VERSION_PREFIX + UUID.randomUUID().toString();
        ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);
        ConnectionPool dataPool = context.getDataConnectionPool(bucketName, key);

        AtomicLong totalSize = new AtomicLong(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        final int chunkSize = context.getPostgresConfig().getChunkSize();
        final int writeConcurrency = context.getPostgresConfig().getWriteConcurrency();

        final MessageDigest fileDigest;
        try {
            fileDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return Mono.error(new RuntimeException("MD5 not available", e));
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(chunkSize);

        // 1. 写入新版本的 chunks（不删除旧数据）
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
                    String checksum = bytesToHex(calculateMd5(chunkData));
                    return new ChunkInfo(idx, chunkData, checksum);
                })
                .limitRate(writeConcurrency + 1)
                .flatMap(chunkInfo -> insertChunk(context, dataPool, shardInfo.getChunkTable(),
                        bucketName, key, uploadVersion, chunkInfo)
                        .doFinally(s -> chunkInfo.clearData()), writeConcurrency)
                .then(Mono.defer(() -> {
                    String etag;
                    synchronized (fileDigest) {
                        etag = "\"" + bytesToHex(fileDigest.digest()) + "\"";
                    }
                    Instant now = Instant.now();

                    // 2. Atomically update/insert object metadata (using upsert)
                    return upsertS3Object(context, bucketName, key, totalSize.get(), etag,
                            op.getContentType(), chunkIndex.get(), op.getMetadata(), shardInfo, uploadVersion)
                            .flatMap(oldVersion -> {
                                // 3. Asynchronously delete old version chunks
                                if (oldVersion != null && !oldVersion.equals(uploadVersion)) {
                                    deleteOldVersionChunks(context, dataPool, shardInfo.getChunkTable(),
                                            bucketName, key, oldVersion)
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .subscribe(
                                                    v -> {
                                                    },
                                                    err -> log.warn("Failed to cleanup old chunks for {}/{}: {}",
                                                            bucketName, key, err.getMessage()));
                                }
                                return Mono.just(S3Object.builder()
                                        .bucketName(bucketName)
                                        .key(key)
                                        .size(totalSize.get())
                                        .etag(etag)
                                        .contentType(op.getContentType() != null ? op.getContentType()
                                                : "application/octet-stream")
                                        .lastModified(now)
                                        .storageClass("STANDARD")
                                        .build());
                            });
                }));
    }

    private Mono<Void> insertChunk(PostgresDriverContext context, ConnectionPool pool, String tableName,
            String bucketName, String key, String uploadVersion, ChunkInfo chunkInfo) {
        // Use upload_id field to store version number (it is part of the unique index)
        String sql = "INSERT INTO " + tableName +
                " (bucket_name, object_key, upload_id, part_number, chunk_index, chunk_data, chunk_size, checksum) " +
                "VALUES ($1, $2, $3, 0, $4, $5, $6, $7)";

        return context.executeUpdate(pool, sql, bucketName, key, uploadVersion, chunkInfo.index,
                chunkInfo.data, chunkInfo.data.length, chunkInfo.checksum)
                .then();
    }

    /**
     * 原子更新 object 元数据，返回旧版本号（如果存在）
     */
    private Mono<String> upsertS3Object(PostgresDriverContext context, String bucketName, String key,
            long size, String etag, String contentType, int chunkCount, Map<String, String> userMetadata,
            ShardingRouter.ShardInfo shardInfo, String uploadVersion) {

        // Convert user metadata to JSON string
        String jsonMetadata = null;
        if (userMetadata != null && !userMetadata.isEmpty()) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
                if (!first)
                    sb.append(",");
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":\"")
                        .append(escapeJson(entry.getValue())).append("\"");
                first = false;
            }
            sb.append("}");
            jsonMetadata = sb.toString();
        }

        // 先查询旧版本号
        String selectSql = "SELECT upload_version FROM t_object WHERE bucket_name = $1 AND object_key = $2";
        final String finalJsonMetadata = jsonMetadata;

        return context.executeQuery(context.getMetaConnectionPool(), selectSql, bucketName, key)
                .next()
                .map(row -> row.get("upload_version", String.class))
                .defaultIfEmpty("")
                .flatMap(oldVersion -> {
                    // Use PostgreSQL UPSERT (ON CONFLICT ... DO UPDATE)
                    String upsertSql = "INSERT INTO t_object " +
                            "(bucket_name, object_key, size, etag, content_type, last_modified, chunk_count, " +
                            "chunk_size, data_db_index, user_metadata, upload_version) " +
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11) " +
                            "ON CONFLICT (bucket_name, object_key) DO UPDATE SET " +
                            "size = $3, etag = $4, content_type = $5, last_modified = $6, chunk_count = $7, " +
                            "chunk_size = $8, data_db_index = $9, user_metadata = $10::jsonb, upload_version = $11";

                    return context.executeUpdate(context.getMetaConnectionPool(), upsertSql,
                            bucketName, key, size, etag,
                            contentType != null ? contentType : "application/octet-stream",
                            LocalDateTime.now(), chunkCount,
                            context.getPostgresConfig().getChunkSize(),
                            shardInfo.getDataDbIndex(), finalJsonMetadata, uploadVersion)
                            .then(Mono.just(oldVersion));
                });
    }

    /**
     * Asynchronously delete old version chunks
     */
    private Mono<Void> deleteOldVersionChunks(PostgresDriverContext context, ConnectionPool pool,
            String tableName, String bucketName, String key, String oldVersion) {
        String sql = "DELETE FROM " + tableName + " WHERE bucket_name = $1 AND object_key = $2 AND upload_id = $3";
        return context.executeUpdate(pool, sql, bucketName, key, oldVersion)
                .doOnSuccess(count -> log.debug("Cleaned up {} old chunks for {}/{} version {}",
                        count, bucketName, key, oldVersion))
                .then();
    }

    private String escapeJson(String str) {
        if (str == null)
            return "";
        return str.replace("\\", "\\\\").replace("\"", "\\\"");
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
