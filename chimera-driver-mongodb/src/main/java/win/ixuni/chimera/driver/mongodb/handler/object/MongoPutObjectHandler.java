package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoDB PutObject 处理器
 * <p>
 * Uses versioning strategy to handle concurrent writes:
 * 1. Assign a unique uploadVersion per upload (stored in the uploadId field)
 * 2. 先插入新版本的 chunks（不删除旧数据）
 * 3. 原子更新 object 元数据指向新版本
 * 4. Asynchronously clean up old version chunks
 * <p>
 * Note: uses the uploadId field to store the version number since it is part of the unique index (bucketName, objectKey, uploadId,
 * partNumber, chunkIndex)，
 * This way different versions of chunks can coexist without unique index conflicts.
 */
@Slf4j
public class MongoPutObjectHandler extends AbstractMongoHandler<PutObjectOperation, S3Object> {

    // Version prefix for regular PUT operations, to distinguish from multipart uploads
    private static final String VERSION_PREFIX = "v:";

    @Override
    protected Mono<S3Object> doHandle(PutObjectOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 1. 检查 Bucket 是否存在
        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return collectAndStoreObject(context, operation);
                });
    }

    private Mono<S3Object> collectAndStoreObject(MongoDriverContext context, PutObjectOperation op) {
        String bucketName = op.getBucketName();
        String key = op.getKey();
        // Generate unique version number for this upload (stored in uploadId field)
        String uploadVersion = VERSION_PREFIX + UUID.randomUUID().toString();
        MongoCollection<Document> chunkCollection = context.getChunkCollection(bucketName, key);

        AtomicLong totalSize = new AtomicLong(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        final int chunkSize = context.getMongoConfig().getChunkSize();
        final int writeConcurrency = context.getMongoConfig().getWriteConcurrency();

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
                .flatMap(chunkInfo -> insertChunk(chunkCollection, bucketName, key, uploadVersion, chunkInfo)
                        .doFinally(s -> chunkInfo.clearData()), writeConcurrency)
                .then(Mono.defer(() -> {
                    String etag;
                    synchronized (fileDigest) {
                        etag = "\"" + bytesToHex(fileDigest.digest()) + "\"";
                    }
                    Instant now = Instant.now();

                    // 2. Atomically update/insert object metadata (using upsert)
                    return upsertS3Object(context, bucketName, key, totalSize.get(), etag,
                            op.getContentType(), chunkIndex.get(), op.getMetadata(), uploadVersion)
                            .flatMap(oldVersion -> {
                                // 3. Asynchronously delete old version chunks
                                if (oldVersion != null && !oldVersion.equals(uploadVersion)) {
                                    deleteOldVersionChunks(chunkCollection, bucketName, key, oldVersion)
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

    private Mono<Void> insertChunk(MongoCollection<Document> collection,
            String bucketName, String key, String uploadVersion, ChunkInfo chunkInfo) {
        // Use uploadId field to store version number (it is part of the unique index)
        Document doc = new Document()
                .append("bucketName", bucketName)
                .append("objectKey", key)
                .append("uploadId", uploadVersion) // Use uploadId to store version number
                .append("partNumber", 0)
                .append("chunkIndex", chunkInfo.index)
                .append("chunkData", new Binary(chunkInfo.data))
                .append("chunkSize", chunkInfo.data.length)
                .append("checksum", chunkInfo.checksum)
                .append("createdAt", new Date());

        return Mono.from(collection.insertOne(doc)).then();
    }

    /**
     * 原子更新 object 元数据，返回旧版本号（如果存在）
     */
    private Mono<String> upsertS3Object(MongoDriverContext context, String bucketName, String key,
            long size, String etag, String contentType, int chunkCount,
            Map<String, String> userMetadata, String uploadVersion) {

        // 先查询旧版本号
        return Mono.from(context.getObjectCollection()
                .find(Filters.and(
                        Filters.eq("bucketName", bucketName),
                        Filters.eq("objectKey", key)))
                .first())
                .map(doc -> doc.getString("uploadVersion"))
                .defaultIfEmpty("")
                .flatMap(oldVersion -> {
                    Document doc = new Document()
                            .append("bucketName", bucketName)
                            .append("objectKey", key)
                            .append("size", size)
                            .append("etag", etag)
                            .append("contentType", contentType != null ? contentType : "application/octet-stream")
                            .append("lastModified", new Date())
                            .append("chunkCount", chunkCount)
                            .append("chunkSize", context.getMongoConfig().getChunkSize())
                            .append("storageClass", "STANDARD")
                            .append("uploadVersion", uploadVersion); // 记录当前版本

                    if (userMetadata != null && !userMetadata.isEmpty()) {
                        doc.append("userMetadata", new Document(userMetadata));
                    }

                    return Mono.from(context.getObjectCollection()
                            .replaceOne(
                                    Filters.and(
                                            Filters.eq("bucketName", bucketName),
                                            Filters.eq("objectKey", key)),
                                    doc,
                                    new ReplaceOptions().upsert(true)))
                            // Add retry mechanism to handle concurrent write conflicts
                            .retryWhen(Retry.backoff(3, java.time.Duration.ofMillis(50))
                                    .doBeforeRetry(signal -> log.debug("Retrying upsert for {}/{}: {}",
                                            bucketName, key, signal.failure().getMessage())))
                            .thenReturn(oldVersion);
                });
    }

    /**
     * Asynchronously delete old version chunks
     */
    private Mono<Void> deleteOldVersionChunks(MongoCollection<Document> collection,
            String bucketName, String key, String oldVersion) {
        return Mono.from(collection.deleteMany(Filters.and(
                Filters.eq("bucketName", bucketName),
                Filters.eq("objectKey", key),
                Filters.eq("uploadId", oldVersion)))) // Use uploadId to match version
                .doOnSuccess(result -> log.debug("Cleaned up {} old chunks for {}/{} version {}",
                        result.getDeletedCount(), bucketName, key, oldVersion))
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
