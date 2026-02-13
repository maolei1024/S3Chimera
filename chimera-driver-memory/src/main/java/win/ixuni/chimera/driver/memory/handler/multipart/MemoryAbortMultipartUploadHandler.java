package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 中止分片上传处理器
 */
public class MemoryAbortMultipartUploadHandler
        implements OperationHandler<AbortMultipartUploadOperation, Void> {

    @Override
    public Mono<Void> handle(AbortMultipartUploadOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        ctx.getMultipartUploads().remove(operation.getUploadId());
        return Mono.empty();
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
