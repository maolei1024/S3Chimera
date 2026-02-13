package win.ixuni.chimera.driver.mongodb.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB 列出 Bucket 处理器
 */
@Slf4j
public class MongoListBucketsHandler extends AbstractMongoHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    protected Mono<Flux<S3Bucket>> doHandle(ListBucketsOperation operation, MongoDriverContext context) {
        Flux<S3Bucket> buckets = Flux.from(context.getBucketCollection().find())
                .map(doc -> S3Bucket.builder()
                        .name(doc.getString("bucketName"))
                        .creationDate(context.toInstant(doc.getDate("creationDate")))
                        .build());

        return Mono.just(buckets);
    }

    @Override
    public Class<ListBucketsOperation> getOperationType() {
        return ListBucketsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
