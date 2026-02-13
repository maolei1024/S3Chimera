package win.ixuni.chimera.driver.local.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统检查 Bucket 是否存在处理器
 */
public class LocalBucketExistsHandler implements OperationHandler<BucketExistsOperation, Boolean> {

    @Override
    public Mono<Boolean> handle(BucketExistsOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            var bucketPath = ctx.getBucketPath(bucketName);
            return Files.exists(bucketPath) && Files.isDirectory(bucketPath);
        });
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
