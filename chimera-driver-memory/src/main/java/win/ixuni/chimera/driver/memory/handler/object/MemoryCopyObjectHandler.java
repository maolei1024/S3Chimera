package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.time.Instant;
import java.util.HashMap;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory CopyObject 处理器
 */
public class MemoryCopyObjectHandler implements OperationHandler<CopyObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CopyObjectOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        
        String srcBucket = operation.getSourceBucket();
        String srcKey = operation.getSourceKey();
        String destBucket = operation.getDestinationBucket();
        String destKey = operation.getDestinationKey();

        MemoryDriverContext.ObjectData src = ctx.getObjects().get(ctx.objectKey(srcBucket, srcKey));
        if (src == null) {
            return Mono.error(new ObjectNotFoundException(srcBucket, srcKey));
        }

        var copy = MemoryDriverContext.ObjectData.builder()
                .bucketName(destBucket)
                .key(destKey)
                .data(src.getData().clone())
                .etag(src.getEtag())
                .contentType(src.getContentType())
                .lastModified(Instant.now())
                .metadata(new HashMap<>(src.getMetadata()))
                .build();

        ctx.getObjects().put(ctx.objectKey(destBucket, destKey), copy);

        return Mono.just(S3Object.builder()
                .bucketName(destBucket)
                .key(destKey)
                .size((long) copy.getData().length)
                .etag(copy.getEtag())
                .lastModified(copy.getLastModified())
                .build());
    }

    @Override
    public Class<CopyObjectOperation> getOperationType() {
        return CopyObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.COPY);
    }
}
