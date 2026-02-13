package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.nio.ByteBuffer;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory GetObject 处理器
 */
public class MemoryGetObjectHandler implements OperationHandler<GetObjectOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        MemoryDriverContext.ObjectData objData = ctx.getObjects().get(ctx.objectKey(bucketName, key));
        if (objData == null) {
            return Mono.error(new ObjectNotFoundException(bucketName, key));
        }

        S3Object metadata = S3Object.builder()
                .bucketName(bucketName)
                .key(key)
                .size((long) objData.getData().length)
                .etag(objData.getEtag())
                .contentType(objData.getContentType())
                .lastModified(objData.getLastModified())
                .storageClass("STANDARD")
                .userMetadata(objData.getMetadata())
                .build();

        return Mono.just(S3ObjectData.builder()
                .metadata(metadata)
                .content(Flux.just(ByteBuffer.wrap(objData.getData())))
                .build());
    }

    @Override
    public Class<GetObjectOperation> getOperationType() {
        return GetObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
