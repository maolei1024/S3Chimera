package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.util.ArrayList;
import java.util.List;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory DeleteObjects 批量删除处理器
 */
public class MemoryDeleteObjectsHandler implements OperationHandler<DeleteObjectsOperation, DeleteObjectsResult> {

    @Override
    public Mono<DeleteObjectsResult> handle(DeleteObjectsOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        DeleteObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        // 检查 bucket 是否存在
        if (!ctx.getBuckets().containsKey(bucketName)) {
            return Mono.error(new BucketNotFoundException(bucketName));
        }

        List<DeleteObjectsResult.DeletedObject> deleted = new ArrayList<>();
        for (String key : request.getKeys()) {
            ctx.getObjects().remove(ctx.objectKey(bucketName, key));
            deleted.add(DeleteObjectsResult.DeletedObject.builder().key(key).build());
        }

        return Mono.just(DeleteObjectsResult.builder().deleted(deleted).build());
    }

    @Override
    public Class<DeleteObjectsOperation> getOperationType() {
        return DeleteObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.BATCH_DELETE);
    }
}
