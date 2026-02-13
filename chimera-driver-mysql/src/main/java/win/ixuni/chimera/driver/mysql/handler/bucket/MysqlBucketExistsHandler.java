package win.ixuni.chimera.driver.mysql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 检查 Bucket 存在处理器
 */
@Slf4j
public class MysqlBucketExistsHandler extends AbstractMysqlHandler<BucketExistsOperation, Boolean> {

    @Override
    protected Mono<Boolean> doHandle(BucketExistsOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();

        String sql = "SELECT 1 FROM t_bucket WHERE bucket_name = ? LIMIT 1";
        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> true, bucketName)
                .next()
                .defaultIfEmpty(false);
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
