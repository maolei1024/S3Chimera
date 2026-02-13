package win.ixuni.chimera.driver.webdav.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * WebDAV 检查 Bucket 是否存在处理器
 */
public class WebDavBucketExistsHandler implements OperationHandler<BucketExistsOperation, Boolean> {

    @Override
    public Mono<Boolean> handle(BucketExistsOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            try {
                String bucketUrl = ctx.getBucketUrl(bucketName);
                return ctx.getSardine().exists(bucketUrl);
            } catch (Exception e) {
                return false;
            }
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
