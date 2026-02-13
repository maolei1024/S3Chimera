package win.ixuni.chimera.driver.mongodb.handler.multipart;

import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.core.util.UploadLockManager;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoDB 上传分片处理器
 * 
 * Note: parts with the same uploadId are written to the database serially to avoid lock contention.
 * 不同 uploadId 的 parts 仍可并行上传。
 */
@Slf4j
public class MongoUploadPartHandler extends AbstractMongoHandler<UploadPartOperation, UploadPart> {

    private final UploadLockManager lockManager = UploadLockManager.getInstance();

    @Override
    protected Mono<UploadPart> doHandle(UploadPartOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        // Use uploadId-level lock to prevent concurrent part writes from causing lock contention
        return lockManager.withLock(uploadId, Mono.from(context.getMultipartUploadCollection()
                .find(Filters.eq("uploadId", uploadId))
                .first())
                .switchIfEmpty(Mono.error(new RuntimeException("Upload not found: " + uploadId)))
                .flatMap(uploadDoc -> {
                    MongoCollection<Document> chunkCollection = context.getChunkCollection(bucketName, key);

                    AtomicLong totalSize = new AtomicLong(0);
                    AtomicInteger chunkIndex = new AtomicInteger(0);

                    final int chunkSize = context.getMongoConfig().getChunkSize();
                    final MessageDigest partDigest;
                    try {
                        partDigest = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        return Mono.error(new RuntimeException("MD5 not available", e));
                    }

                    // 1. 先删除此 part 的旧数据
                    return Mono.from(chunkCollection.deleteMany(Filters.and(
                            Filters.eq("bucketName", bucketName),
                            Filters.eq("objectKey", key),
                            Filters.eq("uploadId", uploadId),
                            Filters.eq("partNumber", partNumber))))
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

                                            Document doc = new Document()
                                                    .append("bucketName", bucketName)
                                                    .append("objectKey", key)
                                                    .append("uploadId", uploadId)
                                                    .append("partNumber", partNumber)
                                                    .append("chunkIndex", idx)
                                                    .append("chunkData", new Binary(chunkData))
                                                    .append("chunkSize", chunkData.length)
                                                    .append("createdAt", new Date());

                                            return Mono.from(chunkCollection.insertOne(doc));
                                        })
                                        .then();
                            }))
                            .then(Mono.defer(() -> {
                                String etag;
                                synchronized (partDigest) {
                                    etag = "\"" + bytesToHex(partDigest.digest()) + "\"";
                                }
                                Instant now = Instant.now();

                                // 保存 part 元数据
                                Document partDoc = new Document()
                                        .append("uploadId", uploadId)
                                        .append("partNumber", partNumber)
                                        .append("size", totalSize.get())
                                        .append("etag", etag)
                                        .append("chunkCount", chunkIndex.get())
                                        .append("uploadedAt", new Date());

                                // First delete old part metadata with same uploadId + partNumber (supports re-upload overwrite)
                                return Mono.from(context.getMultipartPartCollection()
                                        .deleteMany(Filters.and(
                                                Filters.eq("uploadId", uploadId),
                                                Filters.eq("partNumber", partNumber))))
                                        .then(Mono.from(context.getMultipartPartCollection().insertOne(partDoc)))
                                        .thenReturn(UploadPart.builder()
                                                .partNumber(partNumber)
                                                .etag(etag)
                                                .size(totalSize.get())
                                                .lastModified(now)
                                                .build());
                            }));
                })
                .doOnNext(part -> log.debug("UploadPart: uploadId={}, partNumber={}, etag={}",
                        uploadId, partNumber, part.getEtag())));
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
