package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoDB CopyObject 处理器
 */
@Slf4j
public class MongoCopyObjectHandler extends AbstractMongoHandler<CopyObjectOperation, S3Object> {

    @Override
    protected Mono<S3Object> doHandle(CopyObjectOperation operation, MongoDriverContext context) {
        String srcBucket = operation.getSourceBucket();
        String srcKey = operation.getSourceKey();
        String destBucket = operation.getDestinationBucket();
        String destKey = operation.getDestinationKey();

        // 1. 获取源对象元数据
        return context.execute(new HeadObjectOperation(srcBucket, srcKey))
                .switchIfEmpty(Mono.error(new ObjectNotFoundException(srcBucket, srcKey)))
                .flatMap(srcMetadata -> {
                    // 查询源对象的 uploadVersion
                    return Mono.from(context.getObjectCollection().find(
                            Filters.and(Filters.eq("bucketName", srcBucket), Filters.eq("objectKey", srcKey)))
                            .first())
                            .defaultIfEmpty(new Document())
                            .map(doc -> doc.getString("uploadVersion")) // 保持 null
                            .flatMap(srcVersion -> {
                                AtomicLong totalSize = new AtomicLong(0);
                                AtomicInteger chunkCount = new AtomicInteger(0);
                                MessageDigest fileDigest;
                                try {
                                    fileDigest = MessageDigest.getInstance("MD5");
                                } catch (NoSuchAlgorithmException e) {
                                    return Mono.error(new RuntimeException("MD5 not available", e));
                                }

                                String newVersion = java.util.UUID.randomUUID().toString();

                                // 2. 先删除目标位置的旧数据
                                return context.execute(new DeleteObjectOperation(destBucket, destKey))
                                        .then(Mono.defer(() -> {
                                            // 3. 复制数据块 (带版本)
                                            return Flux.from(context.getChunkCollection(srcBucket, srcKey)
                                                    .find(Filters.and(
                                                            Filters.eq("bucketName", srcBucket),
                                                            Filters.eq("objectKey", srcKey),
                                                            srcVersion != null
                                                                    ? Filters.eq("uploadId", srcVersion)
                                                                    : Filters.eq("uploadId", null)))
                                                    .sort(Sorts.ascending("partNumber", "chunkIndex")))
                                                    .flatMap(chunkDoc -> {
                                                        Binary binary = chunkDoc.get("chunkData", Binary.class);
                                                        byte[] data = binary.getData();
                                                        totalSize.addAndGet(data.length);
                                                        int idx = chunkCount.getAndIncrement();
                                                        synchronized (fileDigest) {
                                                            fileDigest.update(data);
                                                        }

                                                        Document newChunkDoc = new Document()
                                                                .append("bucketName", destBucket)
                                                                .append("objectKey", destKey)
                                                                .append("uploadId", newVersion) // Use newVersion
                                                                .append("partNumber", 0)
                                                                .append("chunkIndex", idx)
                                                                .append("chunkData", new Binary(data))
                                                                .append("chunkSize", data.length)
                                                                .append("createdAt", new Date());

                                                        return Mono.from(context.getChunkCollection(destBucket, destKey)
                                                                .insertOne(newChunkDoc));
                                                    })
                                                    .then();
                                        }))
                                        .then(Mono.defer(() -> {
                                            String etag;
                                            synchronized (fileDigest) {
                                                etag = "\"" + bytesToHex(fileDigest.digest()) + "\"";
                                            }
                                            Instant now = Instant.now();

                                            // 4. 创建目标对象元数据 (带版本)
                                            Document objectDoc = new Document()
                                                    .append("bucketName", destBucket)
                                                    .append("objectKey", destKey)
                                                    .append("size", totalSize.get())
                                                    .append("etag", etag)
                                                    .append("uploadVersion", newVersion)
                                                    .append("contentType", srcMetadata.getContentType())
                                                    .append("lastModified", new Date())
                                                    .append("storageClass", "STANDARD")
                                                    .append("chunkCount", chunkCount.get())
                                                    .append("chunkSize", context.getMongoConfig().getChunkSize());

                                            return Mono.from(context.getObjectCollection().insertOne(objectDoc))
                                                    .thenReturn(S3Object.builder()
                                                            .bucketName(destBucket)
                                                            .key(destKey)
                                                            .size(totalSize.get())
                                                            .etag(etag)
                                                            .contentType(srcMetadata.getContentType())
                                                            .lastModified(now)
                                                            .storageClass("STANDARD")
                                                            .build());
                                        }));
                            });
                }).doOnSuccess(obj -> log.debug("CopyObject: {}:{} -> {}:{}", srcBucket, srcKey, destBucket, destKey));

    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Class<CopyObjectOperation> getOperationType() {
        return CopyObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.READ, Capability.COPY);
    }
}
