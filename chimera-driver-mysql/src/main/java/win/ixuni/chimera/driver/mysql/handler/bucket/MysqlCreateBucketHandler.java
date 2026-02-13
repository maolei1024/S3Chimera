package win.ixuni.chimera.driver.mysql.handler.bucket;

import io.r2dbc.spi.Row;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 创建 Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
@Slf4j
public class MysqlCreateBucketHandler extends AbstractMysqlHandler<CreateBucketOperation, S3Bucket> {

    @Override
    protected Mono<S3Bucket> doHandle(CreateBucketOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();

        // Use INSERT IGNORE for idempotent behavior
        String insertSql = "INSERT IGNORE INTO t_bucket (bucket_name, creation_date) VALUES (?, ?)";
        String selectSql = "SELECT bucket_name, creation_date FROM t_bucket WHERE bucket_name = ?";
        Instant now = Instant.now();

        return Mono.usingWhen(
                Mono.from(ctx.getMetaConnectionPool().create()),
                connection -> Mono.from(connection.createStatement(insertSql)
                        .bind(0, bucketName)
                        .bind(1, LocalDateTime.ofInstant(now, ZoneId.systemDefault()))
                        .execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(Mono.from(connection.createStatement(selectSql)
                                .bind(0, bucketName)
                                .execute()))
                        .flatMap(result -> Mono.from(result.map((row, metadata) -> mapRowToBucket(row)))),
                connection -> Mono.from(connection.close()));
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
