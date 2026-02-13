package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.time.Instant;
import java.util.UUID;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 创建分片上传处理器
 */
public class MemoryCreateMultipartUploadHandler
        implements OperationHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    public Mono<MultipartUpload> handle(CreateMultipartUploadOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 检查 bucket 是否存在
        if (!ctx.getBuckets().containsKey(bucketName)) {
            return Mono.error(new BucketNotFoundException(bucketName));
        }

        String uploadId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        var state = MemoryDriverContext.MultipartState.builder()
                .uploadId(uploadId)
                .bucketName(bucketName)
                .key(key)
                .contentType(operation.getContentType())
                .metadata(operation.getMetadata())
                .initiated(now)
                .build();

        ctx.getMultipartUploads().put(uploadId, state);

        return Mono.just(MultipartUpload.builder()
                .uploadId(uploadId)
                .bucketName(bucketName)
                .key(key)
                .initiated(now)
                .build());
    }

    @Override
    public Class<CreateMultipartUploadOperation> getOperationType() {
        return CreateMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
