package win.ixuni.chimera.driver.memory.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.driver.memory.handler.AbstractMemoryHandler;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory Bucket 存在检查处理器
 */
public class MemoryBucketExistsHandler extends AbstractMemoryHandler<BucketExistsOperation, Boolean> {

    @Override
    protected Mono<Boolean> doHandle(BucketExistsOperation operation, MemoryDriverContext ctx) {
        return Mono.just(ctx.getBuckets().containsKey(operation.getBucketName()));
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
