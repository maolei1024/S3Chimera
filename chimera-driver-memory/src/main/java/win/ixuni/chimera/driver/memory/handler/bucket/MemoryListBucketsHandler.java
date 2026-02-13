package win.ixuni.chimera.driver.memory.handler.bucket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 列出 Buckets 处理器
 */
public class MemoryListBucketsHandler implements OperationHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    public Mono<Flux<S3Bucket>> handle(ListBucketsOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;

        Flux<S3Bucket> buckets = Flux.fromIterable(ctx.getBuckets().values())
                .map(info -> S3Bucket.builder()
                        .name(info.getName())
                        .creationDate(info.getCreationDate())
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
