package win.ixuni.chimera.driver.postgresql.handler.bucket;

import io.r2dbc.spi.Row;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL 创建 S3Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
@Slf4j
public class PostgresCreateBucketHandler extends AbstractPostgresHandler<CreateBucketOperation, S3Bucket> {

    @Override
    protected Mono<S3Bucket> doHandle(CreateBucketOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();

        // Use INSERT ON CONFLICT DO NOTHING for idempotent behavior, then query to return bucket info
        String insertSql = "INSERT INTO t_bucket (bucket_name, creation_date) VALUES ($1, $2) ON CONFLICT (bucket_name) DO NOTHING";
        String selectSql = "SELECT bucket_name, creation_date FROM t_bucket WHERE bucket_name = $1";

        return context.executeUpdate(context.getMetaConnectionPool(), insertSql, bucketName, LocalDateTime.now())
                .then(context.executeQuery(context.getMetaConnectionPool(), selectSql, bucketName)
                        .map(this::mapRowToBucket)
                        .next())
                .doOnSuccess(bucket -> log.debug("S3Bucket 创建/获取成功: {}", bucketName));
    }

    private S3Bucket mapRowToBucket(Row row) {
        String name = row.get("bucket_name", String.class);
        LocalDateTime creationDate = row.get("creation_date", LocalDateTime.class);
        Instant instant = creationDate != null
                ? creationDate.atZone(ZoneId.systemDefault()).toInstant()
                : Instant.now();
        return S3Bucket.builder()
                .name(name)
                .creationDate(instant)
                .build();
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
