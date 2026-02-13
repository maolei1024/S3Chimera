package win.ixuni.chimera.driver.postgresql.handler.multipart;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.NoSuchUploadException;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.core.util.UploadLockManager;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL 上传分片处理器
 * 
 * Note: parts with the same uploadId are written to the database serially to avoid row lock contention.
 * 不同 uploadId 的 parts 仍可并行上传。
 */
@Slf4j
public class PostgresUploadPartHandler extends AbstractPostgresHandler<UploadPartOperation, UploadPart> {

    private final UploadLockManager lockManager = UploadLockManager.getInstance();

    @Override
    protected Mono<UploadPart> doHandle(UploadPartOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        // 先验证 uploadId 是否存在
        return validateUploadId(context, uploadId)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new NoSuchUploadException(uploadId));
                    }
                    // Use uploadId-level lock to prevent concurrent part writes from causing lock contention
                    return lockManager.withLock(uploadId,
                            doUploadPartInternal(operation, context, bucketName, key, uploadId, partNumber));
                });
    }

    private Mono<Boolean> validateUploadId(PostgresDriverContext context, String uploadId) {
        String sql = "SELECT 1 FROM t_multipart_upload WHERE upload_id = $1 AND status = 0 LIMIT 1";
        return context.executeQuery(context.getMetaConnectionPool(), sql, uploadId)
                .map(row -> true)
                .next()
                .defaultIfEmpty(false);
    }

    private Mono<UploadPart> doUploadPartInternal(UploadPartOperation operation, PostgresDriverContext context,
            String bucketName, String key, String uploadId, int partNumber) {
        ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);
        ConnectionPool dataPool = context.getDataConnectionPool(bucketName, key);

        AtomicLong totalSize = new AtomicLong(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        final int chunkSize = context.getPostgresConfig().getChunkSize();
        final MessageDigest partDigest;
        try {
            partDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return Mono.error(new RuntimeException("MD5 not available", e));
        }

        // 1. 先删除此 part 的旧数据
        String deleteChunkSql = "DELETE FROM " + shardInfo.getChunkTable() +
                " WHERE bucket_name = $1 AND object_key = $2 AND upload_id = $3 AND part_number = $4";

        return context.executeUpdate(dataPool, deleteChunkSql, bucketName, key, uploadId, partNumber)
                .then(Mono.defer(() -> {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream(chunkSize);

                    return operation.getContent()
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
                            .flatMap(chunkData -> {
                                totalSize.addAndGet(chunkData.length);
                                int idx = chunkIndex.getAndIncrement();
                                synchronized (partDigest) {
                                    partDigest.update(chunkData);
                                }

                                String insertSql = "INSERT INTO " + shardInfo.getChunkTable() +
                                        " (bucket_name, object_key, upload_id, part_number, chunk_index, " +
                                        "chunk_data, chunk_size) VALUES ($1, $2, $3, $4, $5, $6, $7)";

                                return context.executeUpdate(dataPool, insertSql,
                                        bucketName, key, uploadId, partNumber, idx, chunkData, chunkData.length);
                            })
                            .then();
                }))
                .then(Mono.defer(() -> {
                    String etag;
                    synchronized (partDigest) {
                        etag = "\"" + bytesToHex(partDigest.digest()) + "\"";
                    }
                    Instant now = Instant.now();

                    String insertPartSql = "INSERT INTO t_multipart_part " +
                            "(upload_id, part_number, size, etag, chunk_count, uploaded_at) " +
                            "VALUES ($1, $2, $3, $4, $5, $6) " +
                            "ON CONFLICT (upload_id, part_number) DO UPDATE SET " +
                            "size = $3, etag = $4, chunk_count = $5, uploaded_at = $6";

                    return context.executeUpdate(context.getMetaConnectionPool(), insertPartSql,
                            uploadId, partNumber, totalSize.get(), etag, chunkIndex.get(), LocalDateTime.now())
                            .thenReturn(UploadPart.builder()
                                    .partNumber(partNumber)
                                    .etag(etag)
                                    .size(totalSize.get())
                                    .lastModified(now)
                                    .build());
                }))
                .doOnNext(part -> log.debug("UploadPart: uploadId={}, partNumber={}, etag={}",
                        uploadId, partNumber, part.getEtag()));
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Class<UploadPartOperation> getOperationType() {
        return UploadPartOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
    }
}
