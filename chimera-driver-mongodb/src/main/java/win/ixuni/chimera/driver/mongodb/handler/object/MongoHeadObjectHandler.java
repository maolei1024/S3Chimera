package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * MongoDB HeadObject 处理器
 */
@Slf4j
public class MongoHeadObjectHandler extends AbstractMongoHandler<HeadObjectOperation, S3Object> {

    @Override
    protected Mono<S3Object> doHandle(HeadObjectOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.from(context.getObjectCollection()
                .find(Filters.and(
                        Filters.eq("bucketName", bucketName),
                        Filters.eq("objectKey", key)))
                .first())
                .switchIfEmpty(Mono.error(new ObjectNotFoundException(bucketName, key)))
                .map(doc -> buildS3Object(doc, bucketName, key, context))
                .doOnNext(obj -> log.debug("HeadObject: bucket={}, key={}, size={}",
                        bucketName, key, obj.getSize()));
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
    public Class<HeadObjectOperation> getOperationType() {
        return HeadObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
