package win.ixuni.chimera.driver.postgresql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL S3Bucket 存在检查处理器
 */
@Slf4j
public class PostgresBucketExistsHandler extends AbstractPostgresHandler<BucketExistsOperation, Boolean> {

    @Override
    protected Mono<Boolean> doHandle(BucketExistsOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();

        return context.executeQuery(context.getMetaConnectionPool(),
                "SELECT 1 FROM t_bucket WHERE bucket_name = $1", bucketName)
                .next()
                .map(row -> true)
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
