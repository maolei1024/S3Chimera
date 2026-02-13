package win.ixuni.chimera.driver.mysql.handler.multipart;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 完成分片上传处理器
 */
@Slf4j
public class MysqlCompleteMultipartUploadHandler
                extends AbstractMysqlHandler<CompleteMultipartUploadOperation, S3Object> {

        @Override
        protected Mono<S3Object> doHandle(CompleteMultipartUploadOperation operation, MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();
                String uploadId = operation.getUploadId();
                List<CompletedPart> parts = operation.getParts();

                ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);
                ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

                // 按 partNumber 排序
                List<CompletedPart> sortedParts = parts.stream()
                                .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                                .toList();

                // Batch query all parts info (O(1) optimization)
                return getAllPartInfos(ctx, uploadId, sortedParts)
                                .flatMap(partInfoMap -> {
                                        // 计算总大小和收集 ETag
                                        long totalSize = 0;
                                        int totalChunks = 0;
                                        List<String> partEtags = new ArrayList<>();

                                        for (CompletedPart part : sortedParts) {
                                                PartInfo info = partInfoMap.get(part.getPartNumber());
                                                if (info == null) {
                                                        return Mono.<S3Object>error(new IllegalArgumentException(
                                                                        "Part not found: uploadId=" + uploadId +
                                                                                        ", partNumber="
                                                                                        + part.getPartNumber()));
                                                }
                                                totalSize += info.size;
                                                totalChunks += info.chunkCount;
                                                partEtags.add(info.etag.replace("\"", ""));
                                        }

                                        final long finalTotalSize = totalSize;
                                        final int finalTotalChunks = totalChunks;
                                        String etag = "\"" + calculateMultipartETag(partEtags) + "-"
                                                        + sortedParts.size() + "\"";
                                        Instant now = Instant.now();

                                        // 0. Delete old object chunks first (if any) to avoid unique constraint
                                        // conflicts
                                        return deleteOldObjectChunks(ctx, dataPool, shardInfo.getChunkTable(),
                                                        bucketName, key)
                                                        // 1. 删除旧对象的元数据（如果存在）
                                                        .then(deleteOldObjectMetadata(ctx, shardInfo, bucketName, key))
                                                        // 2. 重新编排 chunk_index 并将 chunks 标记为完成
                                                        .then(finalizeMultipartChunks(ctx, dataPool,
                                                                        shardInfo.getChunkTable(),
                                                                        bucketName, key, uploadId, sortedParts))
                                                        // 3. 查询 content-type 并创建对象元数据
                                                        .then(getContentType(ctx, uploadId))
                                                        .flatMap(contentType -> insertObjectMetadata(ctx, bucketName,
                                                                        key,
                                                                        finalTotalSize, etag, contentType,
                                                                        finalTotalChunks, shardInfo))
                                                        // 4. 清理 multipart 相关记录
                                                        .then(cleanupMultipartUpload(ctx, uploadId))
                                                        .thenReturn(S3Object.builder()
                                                                        .bucketName(bucketName)
                                                                        .key(key)
                                                                        .size(finalTotalSize)
                                                                        .etag(etag)
                                                                        .lastModified(now)
                                                                        .storageClass("STANDARD")
                                                                        .build());
                                });
        }

        private Mono<Void> deleteOldObjectChunks(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
                        String bucketName, String key) {
                String sql = "DELETE FROM " + tableName
                                + " WHERE bucket_name = ? AND object_key = ? AND upload_id IS NULL";
                return ctx.executeUpdate(pool, sql, bucketName, key).then();
        }

        private Mono<Void> deleteOldObjectMetadata(MysqlDriverContext ctx, ShardingRouter.ShardInfo shardInfo,
                        String bucketName, String key) {
                // 删除旧对象元数据
                String deleteObjSql = "DELETE FROM " + shardInfo.getObjectTable()
                                + " WHERE bucket_name = ? AND object_key = ?";
                String deleteMetaSql = "DELETE FROM " + shardInfo.getObjectMetadataTable()
                                + " WHERE bucket_name = ? AND object_key = ?";

                return ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteObjSql, bucketName, key)
                                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(), deleteMetaSql, bucketName, key))
                                .then();
        }

        private record PartInfo(long size, int chunkCount, String etag) {
        }

        /**
         * Batch query all parts info (O(1) optimization)
         * Uses a single SQL + IN clause to avoid N network round-trips
         */
        private Mono<java.util.Map<Integer, PartInfo>> getAllPartInfos(MysqlDriverContext ctx, String uploadId,
                        List<CompletedPart> sortedParts) {
                String partNumbers = sortedParts.stream()
                                .map(p -> String.valueOf(p.getPartNumber()))
                                .collect(Collectors.joining(","));

                String sql = "SELECT part_number, size, chunk_count, etag FROM t_multipart_part " +
                                "WHERE upload_id = ? AND part_number IN (" + partNumbers + ")";

                return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                                row -> java.util.Map.entry(
                                                row.get("part_number", Integer.class),
                                                new PartInfo(
                                                                row.get("size", Long.class),
                                                                row.get("chunk_count", Integer.class),
                                                                row.get("etag", String.class))),
                                uploadId)
                                .collectMap(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue);
        }

        /**
         * Mark chunks as completed (O(parts) complexity optimized version)
         * 
         * Optimization notes:
         * - No longer re-sequencing chunk_index (this was the root cause of large file
         * upload timeouts)
         * - 保留 part_number 不清零，读取时按 (part_number, chunk_index) 排序
         * - Each part needs only one UPDATE statement, reducing complexity from
         * O(chunks) to O(parts)
         * 
         * 向后兼容：
         * - Legacy data has part_number = 0; ORDER BY part_number, chunk_index is
         * equivalent to ORDER BY
         * chunk_index
         */
        private Mono<Void> finalizeMultipartChunks(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
                        String bucketName, String key, String uploadId, List<CompletedPart> sortedParts) {

                log.info("Finalizing multipart upload: uploadId={}, parts={}", uploadId, sortedParts.size());

                // O(1) implementation: single SQL to batch-mark all part chunks as completed
                // Use IN clause for batch update to avoid N network round-trips
                String partNumbers = sortedParts.stream()
                                .map(p -> String.valueOf(p.getPartNumber()))
                                .collect(java.util.stream.Collectors.joining(","));

                String sql = "UPDATE " + tableName +
                                " SET upload_id = NULL " +
                                " WHERE bucket_name = ? AND object_key = ? " +
                                " AND upload_id = ? AND part_number IN (" + partNumbers + ")";

                return ctx.executeUpdate(pool, sql, bucketName, key, uploadId)
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

        private Mono<Void> insertObjectMetadata(MysqlDriverContext ctx, String bucketName, String key,
                        long size, String etag, String contentType, int chunkCount,
                        ShardingRouter.ShardInfo shardInfo) {
                String sql = "INSERT INTO " + shardInfo.getObjectTable() +
                                " (bucket_name, object_key, size, etag, content_type, last_modified, chunk_count, chunk_size, data_db_index) "
                                +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

                return ctx.executeUpdate(ctx.getMetaConnectionPool(), sql,
                                bucketName, key, size, etag,
                                contentType != null ? contentType : "application/octet-stream",
                                LocalDateTime.now(), chunkCount, ctx.getMysqlConfig().getChunkSize(),
                                shardInfo.getDataDbIndex()).then();
        }

        private Mono<Void> cleanupMultipartUpload(MysqlDriverContext ctx, String uploadId) {
                return ctx.executeUpdate(ctx.getMetaConnectionPool(),
                                "DELETE FROM t_multipart_part WHERE upload_id = ?", uploadId)
                                .then(ctx.executeUpdate(ctx.getMetaConnectionPool(),
                                                "DELETE FROM t_multipart_upload WHERE upload_id = ?", uploadId))
                                .then();
        }

        private Mono<String> getContentType(MysqlDriverContext ctx, String uploadId) {
                String sql = "SELECT content_type FROM t_multipart_upload WHERE upload_id = ?";
                return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                                row -> row.get("content_type", String.class), uploadId)
                                .next()
                                .defaultIfEmpty("application/octet-stream");
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
                return EnumSet.of(Capability.MULTIPART_UPLOAD);
        }
}
