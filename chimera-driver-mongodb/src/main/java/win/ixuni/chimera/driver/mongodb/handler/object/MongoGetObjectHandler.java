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
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * MongoDB GetObject 处理器
 */
@Slf4j
public class MongoGetObjectHandler extends AbstractMongoHandler<GetObjectOperation, S3ObjectData> {

    @Override
    protected Mono<S3ObjectData> doHandle(GetObjectOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 1. 先获取 object 元数据（包含当前版本号）
        return Mono.from(context.getObjectCollection()
                .find(Filters.and(
                        Filters.eq("bucketName", bucketName),
                        Filters.eq("objectKey", key)))
                .first())
                .switchIfEmpty(Mono.error(new ObjectNotFoundException(bucketName, key)))
                .flatMap(doc -> {
                    // 获取当前版本号
                    String uploadVersion = doc.getString("uploadVersion");
                    S3Object metadata = buildS3Object(doc, bucketName, key, context);

                    // 2. 根据版本号读取 chunks
                    // 排序策略：按 (partNumber, chunkIndex) 排序
                    // - 旧数据：partNumber = 0，退化为按 chunkIndex 排序
                    // - 新数据（multipart）：按 part 分组，组内按 chunkIndex 排序
                    Flux<ByteBuffer> content = Flux.from(context.getChunkCollection(bucketName, key)
                            .find(Filters.and(
                                    Filters.eq("bucketName", bucketName),
                                    Filters.eq("objectKey", key),
                                    // Use version number or null to match chunks (backward compatible with old data)
                                    uploadVersion != null
                                            ? Filters.eq("uploadId", uploadVersion)
                                            : Filters.eq("uploadId", null)))
                            .sort(Sorts.ascending("partNumber", "chunkIndex")))
                            .map(chunkDoc -> {
                                Binary binary = chunkDoc.get("chunkData", Binary.class);
                                return ByteBuffer.wrap(binary.getData());
                            });

                    return Mono.just(S3ObjectData.builder()
                            .metadata(metadata)
                            .content(content)
                            .build());
                })
                .doOnNext(obj -> log.debug("GetObject: bucket={}, key={}", bucketName, key));
    }

    private S3Object buildS3Object(Document doc, String bucketName, String key, MongoDriverContext context) {
        Map<String, String> userMetadata = new HashMap<>();
        Document metaDoc = doc.get("userMetadata", Document.class);
        if (metaDoc != null) {
            metaDoc.forEach((k, v) -> userMetadata.put(k, v != null ? v.toString() : null));
        }

        return S3Object.builder()
                .bucketName(bucketName)
                .key(key)
                .size(doc.getLong("size"))
                .contentType(doc.getString("contentType"))
                .etag(doc.getString("etag"))
                .lastModified(context.toInstant(doc.getDate("lastModified")))
                .storageClass(doc.getString("storageClass"))
                .userMetadata(userMetadata)
                .build();
    }

    @Override
    public Class<GetObjectOperation> getOperationType() {
        return GetObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
