package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory HeadObject 处理器
 */
public class MemoryHeadObjectHandler implements OperationHandler<HeadObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(HeadObjectOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        MemoryDriverContext.ObjectData objData = ctx.getObjects().get(ctx.objectKey(bucketName, key));
        if (objData == null) {
            return Mono.error(new ObjectNotFoundException(bucketName, key));
        }

        return Mono.just(S3Object.builder()
                .bucketName(bucketName)
                .key(key)
                .size((long) objData.getData().length)
                .etag(objData.getEtag())
                .contentType(objData.getContentType())
                .lastModified(objData.getLastModified())
                .storageClass("STANDARD")
                .userMetadata(objData.getMetadata())
                .build());
    }

    @Override
    public Class<HeadObjectOperation> getOperationType() {
        return HeadObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
