package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.nio.ByteBuffer;
import java.util.Arrays;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory GetObjectRange 处理器 (Range 请求)
 */
public class MemoryGetObjectRangeHandler implements OperationHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectRangeOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long start = operation.getRangeStart();
        Long end = operation.getRangeEnd();

        var objData = ctx.getObjects().get(ctx.objectKey(bucketName, key));
        if (objData == null) {
            return Mono.error(new ObjectNotFoundException(bucketName, key));
        }

        byte[] data = objData.getData();
        long actualEnd = (end != null ? Math.min(end, data.length - 1) : data.length - 1);
        int length = (int) (actualEnd - start + 1);
        byte[] rangeData = Arrays.copyOfRange(data, (int) start, (int) start + length);

        return Mono.just(S3ObjectData.builder()
                .metadata(S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size((long) length)
                        .etag(objData.getEtag())
                        .contentType(objData.getContentType())
                        .lastModified(objData.getLastModified())
                        .build())
                .content(Flux.just(ByteBuffer.wrap(rangeData)))
                .build());
    }

    @Override
    public Class<GetObjectRangeOperation> getOperationType() {
        return GetObjectRangeOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.RANGE_READ);
    }
}
