package win.ixuni.chimera.driver.postgresql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL 删除 S3Bucket 处理器
 */
@Slf4j
public class PostgresDeleteBucketHandler extends AbstractPostgresHandler<DeleteBucketOperation, Void> {

    @Override
    protected Mono<Void> doHandle(DeleteBucketOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();

        // 1. 检查 S3Bucket 是否存在
        return context.executeQuery(context.getMetaConnectionPool(),
                "SELECT id FROM t_bucket WHERE bucket_name = $1", bucketName)
                .next()
                .switchIfEmpty(Mono.error(new BucketNotFoundException(bucketName)))
                // 2. Check if there are objects
                .flatMap(row -> context.executeQuery(context.getMetaConnectionPool(),
                        "SELECT id FROM t_object WHERE bucket_name = $1 LIMIT 1", bucketName)
                        .next())
                .flatMap(objectRow -> Mono.<Void>error(new BucketNotEmptyException(bucketName)))
                // 3. Check if there are multipart uploads
                .switchIfEmpty(context.executeQuery(context.getMetaConnectionPool(),
                        "SELECT id FROM t_multipart_upload WHERE bucket_name = $1 LIMIT 1", bucketName)
                        .next()
                        .flatMap(uploadRow -> Mono.<Void>error(new BucketNotEmptyException(bucketName))))
                // 4. 删除 S3Bucket
                .switchIfEmpty(context.executeUpdate(context.getMetaConnectionPool(),
                        "DELETE FROM t_bucket WHERE bucket_name = $1", bucketName)
                        .doOnSuccess(count -> log.debug("S3Bucket 删除成功: {}", bucketName))
                        .then());
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
