package win.ixuni.chimera.driver.mongodb.handler.multipart;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB 完成分片上传处理器
 */
@Slf4j
public class MongoCompleteMultipartUploadHandler
                extends AbstractMongoHandler<CompleteMultipartUploadOperation, S3Object> {

        @Override
        protected Mono<S3Object> doHandle(CompleteMultipartUploadOperation operation, MongoDriverContext context) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();
                String uploadId = operation.getUploadId();
                List<CompletedPart> parts = operation.getParts();

                // 按 partNumber 排序
                List<CompletedPart> sortedParts = parts.stream()
                                .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                                .toList();

                // 1. 获取上传会话
                return Mono.from(context.getMultipartUploadCollection()
                                .find(Filters.eq("uploadId", uploadId))
                                .first())
                                .switchIfEmpty(Mono.error(new RuntimeException("Upload not found: " + uploadId)))
                                .flatMap(uploadDoc -> {
                                        String contentType = uploadDoc.getString("contentType");

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

                                                                log.info("Finalizing multipart upload: uploadId={}, parts={}",
                                                                                uploadId, sortedParts.size());

                                                                // O(1) optimization: single updateMany to update all parts
                                                                // Use Filters.in() for batch update, avoiding N network round-trips
                                                                List<Integer> partNumberList = sortedParts.stream()
                                                                                .map(CompletedPart::getPartNumber)
                                                                                .toList();

                                                                return Mono.from(context
                                                                                .getChunkCollection(bucketName, key)
                                                                                .updateMany(
                                                                                                Filters.and(
                                                                                                                Filters.eq("uploadId",
                                                                                                                                uploadId),
                                                                                                                Filters.in("partNumber",
                                                                                                                                partNumberList)),
                                                                                                Updates.set("uploadId",
                                                                                                                null)))
                                                                                .flatMap(result -> {
                                                                                        long totalUpdated = result
                                                                                                        .getModifiedCount();
                                                                                        if (totalUpdated == 0) {
                                                                                                log.error("No chunks found for upload {}",
                                                                                                                uploadId);
                                                                                                return Mono.error(
                                                                                                                new RuntimeException(
                                                                                                                                "No chunks found for multipart upload: "
                                                                                                                                                + uploadId));
                                                                                        }
                                                                                        log.info("Finalized multipart upload: uploadId={}, totalChunks={}",
                                                                                                        uploadId,
                                                                                                        totalUpdated);

                                                                                        // 创建最终对象元数据
                                                                                        Document objectDoc = new Document()
                                                                                                        .append("bucketName",
                                                                                                                        bucketName)
                                                                                                        .append("objectKey",
                                                                                                                        key)
                                                                                                        .append("size", finalTotalSize)
                                                                                                        .append("etag", etag)
                                                                                                        .append("contentType",
                                                                                                                        contentType != null
                                                                                                                                        ? contentType
                                                                                                                                        : "application/octet-stream")
                                                                                                        .append("lastModified",
                                                                                                                        Date.from(now))
                                                                                                        .append("chunkCount",
                                                                                                                        finalTotalChunks)
                                                                                                        .append("chunkSize",
                                                                                                                        context.getMongoConfig()
                                                                                                                                        .getChunkSize())
                                                                                                        .append("storageClass",
                                                                                                                        "STANDARD");

                                                                                        return Mono.from(context
                                                                                                        .getObjectCollection()
                                                                                                        .replaceOne(
                                                                                                                        Filters.and(
                                                                                                                                        Filters.eq("bucketName",
                                                                                                                                                        bucketName),
                                                                                                                                        Filters.eq("objectKey",
                                                                                                                                                        key)),
                                                                                                                        objectDoc,
                                                                                                                        new ReplaceOptions()
                                                                                                                                        .upsert(true)))
                                                                                                        // 清理分片上传会话和
                                                                                                        // parts
                                                                                                        .then(Mono.from(context
                                                                                                                        .getMultipartPartCollection()
                                                                                                                        .deleteMany(Filters
                                                                                                                                        .eq("uploadId", uploadId))))
                                                                                                        .then(Mono.from(context
                                                                                                                        .getMultipartUploadCollection()
                                                                                                                        .deleteOne(Filters
                                                                                                                                        .eq("uploadId", uploadId))))
                                                                                                        .thenReturn(S3Object
                                                                                                                        .builder()
                                                                                                                        .bucketName(bucketName)
                                                                                                                        .key(key)
                                                                                                                        .size(finalTotalSize)
                                                                                                                        .etag(etag)
                                                                                                                        .contentType(contentType)
                                                                                                                        .lastModified(now)
                                                                                                                        .storageClass("STANDARD")
                                                                                                                        .build());
                                                                                });
                                                        });
                                })
                                .doOnNext(obj -> log.debug("CompleteMultipartUpload: bucket={}, key={}, size={}",
                                                bucketName, key, obj.getSize()));
        }

        private record PartInfo(long size, int chunkCount, String etag) {
        }

        /**
         * Batch query all parts info (O(1) optimization)
         * Uses Filters.in() for batch query, avoiding N network round-trips
         */
        private Mono<java.util.Map<Integer, PartInfo>> getAllPartInfos(MongoDriverContext context, String uploadId,
                        List<CompletedPart> sortedParts) {
                List<Integer> partNumberList = sortedParts.stream()
                                .map(CompletedPart::getPartNumber)
                                .toList();

                return Flux.from(context.getMultipartPartCollection()
                                .find(Filters.and(
                                                Filters.eq("uploadId", uploadId),
                                                Filters.in("partNumber", partNumberList))))
                                .map(doc -> java.util.Map.entry(
                                                doc.getInteger("partNumber"),
                                                new PartInfo(
                                                                doc.getLong("size"),
                                                                doc.getInteger("chunkCount"),
                                                                doc.getString("etag"))))
                                .collectMap(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue);
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
