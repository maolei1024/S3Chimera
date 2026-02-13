package win.ixuni.chimera.driver.mysql.handler.multipart;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.core.util.UploadLockManager;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 上传分片处理器
 * 
 * Note: parts with the same uploadId are written to the database serially to avoid MySQL row lock contention.
 * 不同 uploadId 的 parts 仍可并行上传。
 */
@Slf4j
public class MysqlUploadPartHandler extends AbstractMysqlHandler<UploadPartOperation, UploadPart> {

    private final UploadLockManager lockManager = UploadLockManager.getInstance();

    @Override
    protected Mono<UploadPart> doHandle(UploadPartOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        // 先验证 uploadId 是否存在
        return validateUploadId(ctx, uploadId)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new win.ixuni.chimera.core.exception.NoSuchUploadException(uploadId));
                    }
                    // Use uploadId-level lock to prevent concurrent part writes from causing lock contention
                    return lockManager.withLock(uploadId,
                            doUploadPart(ctx, operation, bucketName, key, uploadId, partNumber));
                });
    }

    private Mono<Boolean> validateUploadId(MysqlDriverContext ctx, String uploadId) {
        String sql = "SELECT 1 FROM t_multipart_upload WHERE upload_id = ? AND status = 0 LIMIT 1";
        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> true, uploadId)
                .next()
                .defaultIfEmpty(false);
    }

    private Mono<UploadPart> doUploadPart(MysqlDriverContext ctx, UploadPartOperation operation,
            String bucketName, String key, String uploadId, int partNumber) {
        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);
        ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

        AtomicLong partSize = new AtomicLong(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        final int chunkSize = ctx.getMysqlConfig().getChunkSize();
        final int writeConcurrency = ctx.getMysqlConfig().getWriteConcurrency();

        final MessageDigest partDigest;
        try {
            partDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return Mono.error(new RuntimeException("MD5 not available", e));
        }

        // 先删除该 part 的旧 chunks
        return deletePartChunks(ctx, dataPool, shardInfo.getChunkTable(), bucketName, key, uploadId, partNumber)
                .then(Mono.defer(() -> {
                    // Simplified: collect all data to memory first, then split into chunks
                    return operation.getContent()
                            .reduce(new ByteArrayOutputStream(), (baos, buf) -> {
                                byte[] bytes = new byte[buf.remaining()];
                                buf.get(bytes);
                                baos.write(bytes, 0, bytes.length);
                                return baos;
                            })
                            .map(ByteArrayOutputStream::toByteArray)
                            .flatMap(allData -> {
                                partSize.set(allData.length);
                                synchronized (partDigest) {
                                    partDigest.update(allData);
                                }

                                log.debug("UploadPart: bucket={}, key={}, uploadId={}, partNumber={}, size={}",
                                        bucketName, key, uploadId, partNumber, allData.length);

                                // 切分成 chunks
                                int numChunks = (allData.length + chunkSize - 1) / chunkSize;
                                if (numChunks == 0)
                                    numChunks = 1; // At least one chunk

                                return Flux.range(0, numChunks)
                                        .flatMap(i -> {
                                            int start = i * chunkSize;
                                            int end = Math.min(start + chunkSize, allData.length);
                                            byte[] chunkData = new byte[end - start];
                                            System.arraycopy(allData, start, chunkData, 0, chunkData.length);
                                            String checksum = bytesToHex(calculateMd5(chunkData));
                                            int idx = chunkIndex.getAndIncrement();

                                            return insertChunk(ctx, dataPool, shardInfo.getChunkTable(),
                                                    bucketName, key, uploadId, partNumber, idx, chunkData, checksum);
                                        }, writeConcurrency)
                                        .then();
                            });
                }))
                .then(Mono.defer(() -> {
                    String etag;
                    synchronized (partDigest) {
                        etag = "\"" + bytesToHex(partDigest.digest()) + "\"";
                    }

                    log.debug("UploadPart complete: partNumber={}, etag={}, size={}, chunks={}",
                            partNumber, etag, partSize.get(), chunkIndex.get());

                    // 记录 part 信息
                    return insertPartInfo(ctx, uploadId, partNumber, etag, partSize.get(),
                            ctx.getShardingRouter().getShardInfo(bucketName, key).getDataDbIndex(),
                            chunkIndex.get())
                            .thenReturn(UploadPart.builder()
                                    .partNumber(partNumber)
                                    .etag(etag)
                                    .size(partSize.get())
                                    .lastModified(Instant.now())
                                    .build());
                }));
    }

    private Mono<Void> deletePartChunks(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
            String bucketName, String key, String uploadId, int partNumber) {
        String sql = "DELETE FROM " + tableName +
                " WHERE bucket_name = ? AND object_key = ? AND upload_id = ? AND part_number = ?";
        return ctx.executeUpdate(pool, sql, bucketName, key, uploadId, partNumber).then();
    }

    private Mono<Void> insertChunk(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
            String bucketName, String key, String uploadId, int partNumber,
            int chunkIndex, byte[] data, String checksum) {
        String sql = "INSERT INTO " + tableName +
                " (bucket_name, object_key, upload_id, part_number, chunk_index, chunk_data, chunk_size, checksum) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        return Mono.usingWhen(
                Mono.from(pool.create()),
                connection -> Mono.from(connection.createStatement(sql)
                        .bind(0, bucketName)
                        .bind(1, key)
                        .bind(2, uploadId)
                        .bind(3, partNumber)
                        .bind(4, chunkIndex)
                        .bind(5, data)
                        .bind(6, data.length)
                        .bind(7, checksum)
                        .execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(),
                connection -> Mono.from(connection.close()));
    }

    private Mono<Void> insertPartInfo(MysqlDriverContext ctx, String uploadId, int partNumber,
            String etag, long size, int dataDbIndex, int chunkCount) {
        // 先删除旧记录
        String deleteSql = "DELETE FROM t_multipart_part WHERE upload_id = ? AND part_number = ?";
        String insertSql = "INSERT INTO t_multipart_part (upload_id, part_number, etag, size, data_db_index, chunk_start_id, chunk_count, uploaded_at) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        return ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteSql, uploadId, partNumber)
                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(), insertSql,
                        uploadId, partNumber, etag, size, dataDbIndex, 0L, chunkCount, LocalDateTime.now()))
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

    @Override
    public Class<UploadPartOperation> getOperationType() {
        return UploadPartOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
