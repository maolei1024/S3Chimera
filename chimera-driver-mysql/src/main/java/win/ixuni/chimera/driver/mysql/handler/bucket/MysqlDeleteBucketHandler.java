package win.ixuni.chimera.driver.mysql.handler.bucket;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 删除 Bucket 处理器
 */
@Slf4j
public class MysqlDeleteBucketHandler extends AbstractMysqlHandler<DeleteBucketOperation, Void> {

    @Override
    protected Mono<Void> doHandle(DeleteBucketOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();

        return bucketExists(ctx, bucketName)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return isBucketEmpty(ctx, bucketName);
                })
                .flatMap(empty -> {
                    if (!empty) {
                        return Mono.error(new BucketNotEmptyException(bucketName));
                    }
                    String sql = "DELETE FROM t_bucket WHERE bucket_name = ?";
                    return ctx.executeUpdate(ctx.getMetaConnectionPool(), sql, bucketName).then();
                });
    }

    private Mono<Boolean> bucketExists(MysqlDriverContext ctx, String bucketName) {
        String sql = "SELECT 1 FROM t_bucket WHERE bucket_name = ? LIMIT 1";
        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> true, bucketName)
                .next()
                .defaultIfEmpty(false);
    }

    private Mono<Boolean> isBucketEmpty(MysqlDriverContext ctx, String bucketName) {
        // 检查对象表是否为空（简化实现，检查 t_object 表）
        String sql = "SELECT 1 FROM t_object WHERE bucket_name = ? LIMIT 1";
        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> true, bucketName)
                .next()
                .defaultIfEmpty(false)
                .map(hasObjects -> !hasObjects);
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
