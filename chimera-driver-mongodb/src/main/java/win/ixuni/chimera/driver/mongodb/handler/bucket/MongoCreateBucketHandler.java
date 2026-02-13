package win.ixuni.chimera.driver.mongodb.handler.bucket;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB 创建 S3Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
@Slf4j
public class MongoCreateBucketHandler extends AbstractMongoHandler<CreateBucketOperation, S3Bucket> {

    @Override
    protected Mono<S3Bucket> doHandle(CreateBucketOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        Instant now = Instant.now();

        // Try to insert new bucket; if already exists, ignore and query to return bucket info
        Document doc = new Document()
                .append("bucketName", bucketName)
                .append("creationDate", Date.from(now))
                .append("createdAt", Date.from(now));

        return Mono.from(context.getBucketCollection().insertOne(doc))
                .map(result -> {
                    log.debug("S3Bucket 创建成功: {}", bucketName);
                    return S3Bucket.builder()
                            .name(bucketName)
                            .creationDate(now)
                            .build();
                })
                .onErrorResume(e -> {
                    String msg = e.getMessage();
                    if (msg != null && (msg.contains("duplicate key") || msg.contains("E11000"))) {
                        // Bucket already exists, query and return existing bucket info
                        log.debug("S3Bucket already exists, returning existing bucket: {}", bucketName);
                        return Mono.from(context.getBucketCollection()
                                .find(Filters.eq("bucketName", bucketName))
                                .first())
                                .map(existingDoc -> {
                                    Date creationDate = existingDoc.getDate("creationDate");
                                    return S3Bucket.builder()
                                            .name(bucketName)
                                            .creationDate(creationDate != null ? creationDate.toInstant() : now)
                                            .build();
                                });
                    }
                    return Mono.error(e);
                });
    }

    @Override
    public Class<CreateBucketOperation> getOperationType() {
        return CreateBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
