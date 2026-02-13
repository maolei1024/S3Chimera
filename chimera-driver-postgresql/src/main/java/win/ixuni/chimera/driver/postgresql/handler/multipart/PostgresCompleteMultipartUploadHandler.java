package win.ixuni.chimera.driver.postgresql.handler.multipart;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL 完成分片上传处理器
 */
@Slf4j
public class PostgresCompleteMultipartUploadHandler
                extends AbstractPostgresHandler<CompleteMultipartUploadOperation, S3Object> {

        @Override
        protected Mono<S3Object> doHandle(CompleteMultipartUploadOperation operation, PostgresDriverContext context) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();
                String uploadId = operation.getUploadId();
                List<CompletedPart> parts = operation.getParts();

                // 按 partNumber 排序
                List<CompletedPart> sortedParts = parts.stream()
                                .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                                .toList();

                ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);
                ConnectionPool dataPool = context.getDataConnectionPool(bucketName, key);

                // 1. 获取上传会话
                return context.executeQuery(context.getMetaConnectionPool(),
                                "SELECT content_type FROM t_multipart_upload WHERE upload_id = $1", uploadId)
                                .next()
                                .switchIfEmpty(Mono.error(new RuntimeException("Upload not found: " + uploadId)))
                                .flatMap(uploadRow -> {
                                        String contentType = uploadRow.get("content_type", String.class);

                                        // 2. Batch query all parts info (O(1) optimization)
                                        return getAllPartInfos(context, uploadId, sortedParts)
                                                        .flatMap(partInfoMap -> {
                                                                // 计算总大小和收集 ETag
                                                                long totalSize = 0;
                                                                int totalChunks = 0;
                                                                List<String> partEtags = new ArrayList<>();

                                                                for (CompletedPart part : sortedParts) {
                                                                        PartInfo info = partInfoMap
                                                                                        .get(part.getPartNumber());
                                                                        if (info == null) {
                                                                                return Mono.<S3Object>error(
                                                                                                new IllegalArgumentException(
                                                                                                                "Part not found: uploadId="
                                                                                                                                + uploadId
                                                                                                                                +
                                                                                                                                ", partNumber="
                                                                                                                                + part.getPartNumber()));
                                                                        }
                                                                        totalSize += info.size;
                                                                        totalChunks += info.chunkCount;
                                                                        partEtags.add(info.etag.replace("\"", ""));
                                                                }

                                                                final long finalTotalSize = totalSize;
                                                                final int finalTotalChunks = totalChunks;
                                                                String etag = "\"" + calculateMultipartETag(partEtags)
                                                                                + "-" + sortedParts.size() + "\"";
                                                                Instant now = Instant.now();

                                                                // 3. 删除旧对象的 chunks (如果存在)
                                                                return deleteOldObjectChunks(context, dataPool,
                                                                                shardInfo.getChunkTable(), bucketName,
                                                                                key)
                                                                                // 4. 将 chunks 标记为完成
                                                                                .then(finalizeMultipartChunks(context,
                                                                                                dataPool,
                                                                                                shardInfo.getChunkTable(),
                                                                                                bucketName, key,
                                                                                                uploadId, sortedParts))
                                                                                // 5. 创建对象元数据
                                                                                .then(context.executeUpdate(context
                                                                                                .getMetaConnectionPool(),
                                                                                                "INSERT INTO t_object "
                                                                                                                +
                                                                                                                "(bucket_name, object_key, size, etag, content_type, last_modified, "
                                                                                                                +
                                                                                                                "chunk_count, chunk_size, data_db_index) "
                                                                                                                +
                                                                                                                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                                                                                                bucketName, key,
                                                                                                finalTotalSize, etag,
                                                                                                contentType != null
                                                                                                                ? contentType
                                                                                                                : "application/octet-stream",
                                                                                                LocalDateTime.now(),
                                                                                                finalTotalChunks,
                                                                                                context.getPostgresConfig()
                                                                                                                .getChunkSize(),
                                                                                                shardInfo.getDataDbIndex()))
                                                                                // 6. 清理分片上传会话和 parts
                                                                                .then(context.executeUpdate(context
                                                                                                .getMetaConnectionPool(),
                                                                                                "DELETE FROM t_multipart_part WHERE upload_id = $1",
                                                                                                uploadId))
                                                                                .then(context.executeUpdate(context
                                                                                                .getMetaConnectionPool(),
                                                                                                "DELETE FROM t_multipart_upload WHERE upload_id = $1",
                                                                                                uploadId))
                                                                                .thenReturn(S3Object.builder()
                                                                                                .bucketName(bucketName)
                                                                                                .key(key)
                                                                                                .size(finalTotalSize)
                                                                                                .etag(etag)
                                                                                                .contentType(contentType)
                                                                                                .lastModified(now)
                                                                                                .storageClass("STANDARD")
                                                                                                .build());
                                                        });
                                })
                                .doOnNext(obj -> log.debug("CompleteMultipartUpload: bucket={}, key={}, size={}",
                                                bucketName, key, obj.getSize()));
        }

        private record PartInfo(long size, int chunkCount, String etag) {
        }

        /**
         * Batch query all parts info (O(1) optimization)
         * Uses a single SQL + ANY(ARRAY[...]) syntax to avoid N network round-trips
         */
        private Mono<java.util.Map<Integer, PartInfo>> getAllPartInfos(PostgresDriverContext context, String uploadId,
                        List<CompletedPart> sortedParts) {
                String partNumbers = sortedParts.stream()
                                .map(p -> String.valueOf(p.getPartNumber()))
                                .collect(java.util.stream.Collectors.joining(","));

                String sql = "SELECT part_number, size, chunk_count, etag FROM t_multipart_part " +
                                "WHERE upload_id = $1 AND part_number = ANY(ARRAY[" + partNumbers + "])";

                return context.executeQuery(context.getMetaConnectionPool(), sql, uploadId)
                                .map(row -> java.util.Map.entry(
                                                row.get("part_number", Integer.class),
                                                new PartInfo(
                                                                row.get("size", Long.class),
                                                                row.get("chunk_count", Integer.class),
                                                                row.get("etag", String.class))))
                                .collectMap(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue);
        }

        private Mono<Void> deleteOldObjectChunks(PostgresDriverContext context, ConnectionPool pool, String tableName,
                        String bucketName, String key) {
                String sql = "DELETE FROM " + tableName
                                + " WHERE bucket_name = $1 AND object_key = $2 AND upload_id IS NULL";
                return context.executeUpdate(pool, sql, bucketName, key).then();
        }

        /**
         * Mark chunks as completed (O(1) complexity optimized version)
         * 
         * Optimization notes:
         * - Use a single SQL + ANY(ARRAY[...]) to update all parts at once
         * - 从 N 次网络往返降为 1 次
         * 
         * 向后兼容：
         * - Legacy data has part_number = 0; ORDER BY part_number, chunk_index is equivalent to ORDER BY
         * chunk_index
         */
        private Mono<Void> finalizeMultipartChunks(PostgresDriverContext context, ConnectionPool pool, String tableName,
                        String bucketName, String key, String uploadId, List<CompletedPart> sortedParts) {

                log.info("Finalizing multipart upload: uploadId={}, parts={}", uploadId, sortedParts.size());

                // O(1) implementation: single SQL to batch-mark all part chunks as completed
                // PostgreSQL uses = ANY(ARRAY[...]) syntax
                String partNumbers = sortedParts.stream()
                                .map(p -> String.valueOf(p.getPartNumber()))
                                .collect(java.util.stream.Collectors.joining(","));

                String sql = "UPDATE " + tableName +
                                " SET upload_id = NULL " +
                                " WHERE bucket_name = $1 AND object_key = $2 " +
                                " AND upload_id = $3 AND part_number = ANY(ARRAY[" + partNumbers + "])";

                return context.executeUpdate(pool, sql, bucketName, key, uploadId)
                                .flatMap(totalUpdated -> {
                                        if (totalUpdated == 0) {
                                                log.error("No chunks found for upload {}", uploadId);
                                                return Mono.error(new RuntimeException(
                                                                "No chunks found for multipart upload: " + uploadId));
                                        }
                                        log.info("Finalized multipart upload: uploadId={}, totalChunks={}", uploadId,
                                                        totalUpdated);
                                        return Mono.<Void>empty();
                                });
        }

        private String calculateMultipartETag(List<String> partEtags) {
                try {
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        for (String hexEtag : partEtags) {
                                byte[] partMd5 = hexToBytes(hexEtag);
                                md.update(partMd5);
                        }
                        return bytesToHex(md.digest());
                } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("MD5 not available", e);
                }
        }

        private byte[] hexToBytes(String hex) {
                int len = hex.length();
                byte[] data = new byte[len / 2];
                for (int i = 0; i < len; i += 2) {
                        data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                        + Character.digit(hex.charAt(i + 1), 16));
                }
                return data;
        }

        private String bytesToHex(byte[] bytes) {
                StringBuilder sb = new StringBuilder();
                for (byte b : bytes) {
                        sb.append(String.format("%02x", b));
                }
                return sb.toString();
        }

        @Override
        public Class<CompleteMultipartUploadOperation> getOperationType() {
                return CompleteMultipartUploadOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
        }
}
