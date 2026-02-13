package win.ixuni.chimera.driver.mongodb.handler.bucket;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB 删除 S3Bucket 处理器
 */
@Slf4j
public class MongoDeleteBucketHandler extends AbstractMongoHandler<DeleteBucketOperation, Void> {

    @Override
    protected Mono<Void> doHandle(DeleteBucketOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();

        // 1. 检查 S3Bucket 是否存在
        return Mono.from(context.getBucketCollection()
                .find(Filters.eq("bucketName", bucketName))
                .first())
                .switchIfEmpty(Mono.error(new BucketNotFoundException(bucketName)))
                // 2. 检查 S3Bucket 是否为空
                .flatMap(doc -> Mono.from(context.getObjectCollection()
                        .find(Filters.eq("bucketName", bucketName))
                        .first()))
                .flatMap(objectDoc -> Mono.<Void>error(new BucketNotEmptyException(bucketName)))
                // 3. Check if there are in-progress multipart uploads
                .then(Mono.from(context.getMultipartUploadCollection()
                        .find(Filters.eq("bucketName", bucketName))
                        .first()))
                .flatMap(uploadDoc -> Mono.<Void>error(new BucketNotEmptyException(bucketName)))
                // 4. 删除 S3Bucket
                .then(Mono.from(context.getBucketCollection()
                        .deleteOne(Filters.eq("bucketName", bucketName))))
                .doOnSuccess(result -> log.debug("S3Bucket 删除成功: {}", bucketName))
                .then();
    }

    @Override
    public Class<DeleteBucketOperation> getOperationType() {
        return DeleteBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
