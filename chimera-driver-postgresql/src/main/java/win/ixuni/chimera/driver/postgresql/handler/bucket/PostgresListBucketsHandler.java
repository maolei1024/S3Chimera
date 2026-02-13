package win.ixuni.chimera.driver.postgresql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL 列出 Bucket 处理器
 */
@Slf4j
public class PostgresListBucketsHandler extends AbstractPostgresHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    protected Mono<Flux<S3Bucket>> doHandle(ListBucketsOperation operation, PostgresDriverContext context) {
        Flux<S3Bucket> buckets = context.executeQuery(context.getMetaConnectionPool(),
                "SELECT bucket_name, creation_date FROM t_bucket ORDER BY bucket_name")
                .map(row -> S3Bucket.builder()
                        .name(row.get("bucket_name", String.class))
                        .creationDate(context.toInstant(row.get("creation_date", LocalDateTime.class)))
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
