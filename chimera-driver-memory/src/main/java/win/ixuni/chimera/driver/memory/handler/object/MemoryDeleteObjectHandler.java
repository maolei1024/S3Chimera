package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory DeleteObject 处理器
 */
public class MemoryDeleteObjectHandler implements OperationHandler<DeleteObjectOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteObjectOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        if (!ctx.getBuckets().containsKey(bucketName)) {
            return Mono.error(new BucketNotFoundException(bucketName));
        }

        // S3 删除不存在的对象是幂等的
        ctx.getObjects().remove(ctx.objectKey(bucketName, key));
        return Mono.empty();
    }

    @Override
    public Class<DeleteObjectOperation> getOperationType() {
        return DeleteObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
