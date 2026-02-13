package win.ixuni.chimera.driver.memory.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 删除 Bucket 处理器
 */
public class MemoryDeleteBucketHandler implements OperationHandler<DeleteBucketOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteBucketOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();

        if (!ctx.getBuckets().containsKey(bucketName)) {
            return Mono.error(new BucketNotFoundException(bucketName));
        }

        // Check if there are objects
        boolean hasObjects = ctx.getObjects().keySet().stream()
                .anyMatch(key -> key.startsWith(bucketName + "/"));

        if (hasObjects) {
            return Mono.error(new BucketNotEmptyException(bucketName));
        }

        ctx.getBuckets().remove(bucketName);
        return Mono.empty();
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
