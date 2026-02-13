package win.ixuni.chimera.driver.mongodb.handler.bucket;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB S3Bucket 存在检查处理器
 */
@Slf4j
public class MongoBucketExistsHandler extends AbstractMongoHandler<BucketExistsOperation, Boolean> {

    @Override
    protected Mono<Boolean> doHandle(BucketExistsOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();

        return Mono.from(context.getBucketCollection()
                .find(Filters.eq("bucketName", bucketName))
                .first())
                .map(doc -> true)
                .defaultIfEmpty(false)
                .doOnNext(exists -> log.debug("S3Bucket {} 存在检查: {}", bucketName, exists));
    }

    @Override
    public Class<BucketExistsOperation> getOperationType() {
        return BucketExistsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
