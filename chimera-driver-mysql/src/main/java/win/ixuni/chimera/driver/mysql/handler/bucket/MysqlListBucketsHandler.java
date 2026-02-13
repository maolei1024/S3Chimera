package win.ixuni.chimera.driver.mysql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.LocalDateTime;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 列出 Bucket 处理器
 */
@Slf4j
public class MysqlListBucketsHandler extends AbstractMysqlHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    protected Mono<Flux<S3Bucket>> doHandle(ListBucketsOperation operation, MysqlDriverContext ctx) {
        String sql = "SELECT bucket_name, creation_date FROM t_bucket ORDER BY bucket_name";
        Flux<S3Bucket> buckets = ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> S3Bucket.builder()
                        .name(row.get("bucket_name", String.class))
                        .creationDate(ctx.toInstant(row.get("creation_date", LocalDateTime.class)))
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
