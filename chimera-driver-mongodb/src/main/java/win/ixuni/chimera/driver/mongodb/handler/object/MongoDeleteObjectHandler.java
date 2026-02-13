package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB DeleteObject 处理器
 * <p>
 * Deleting an object removes all versions of chunks (unrestricted by uploadId), ensuring full cleanup.
 */
@Slf4j
public class MongoDeleteObjectHandler extends AbstractMongoHandler<DeleteObjectOperation, Void> {

    @Override
    protected Mono<Void> doHandle(DeleteObjectOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 先检查 Bucket 是否存在
        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return deleteObjectInternal(context, bucketName, key);
                });
    }

    private Mono<Void> deleteObjectInternal(MongoDriverContext context, String bucketName, String key) {
        // 1. Delete all versions of data chunks (unrestricted by uploadId)
        return Mono.from(context.getChunkCollection(bucketName, key)
                .deleteMany(Filters.and(
                        Filters.eq("bucketName", bucketName),
                        Filters.eq("objectKey", key))))
                // 2. 删除对象元数据
                .then(Mono.from(context.getObjectCollection()
                        .deleteOne(Filters.and(
                                Filters.eq("bucketName", bucketName),
                                Filters.eq("objectKey", key)))))
                .doOnSuccess(result -> log.debug("DeleteObject: bucket={}, key={}", bucketName, key))
                .then();
    }

    @Override
    public Class<DeleteObjectOperation> getOperationType() {
        return DeleteObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
