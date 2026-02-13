package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.ListMultipartUploadsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.util.List;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 列出分片上传处理器
 */
public class MemoryListMultipartUploadsHandler
        implements OperationHandler<ListMultipartUploadsOperation, ListMultipartUploadsResult> {

    @Override
    public Mono<ListMultipartUploadsResult> handle(ListMultipartUploadsOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String prefix = operation.getPrefix();

        List<MultipartUpload> uploads = ctx.getMultipartUploads().values().stream()
                .filter(s -> s.getBucketName().equals(bucketName))
                .filter(s -> prefix == null || s.getKey().startsWith(prefix))
                .map(s -> MultipartUpload.builder()
                        .uploadId(s.getUploadId())
                        .bucketName(s.getBucketName())
                        .key(s.getKey())
                        .initiated(s.getInitiated())
                        .build())
                .toList();

        return Mono.just(ListMultipartUploadsResult.builder()
                .bucketName(bucketName)
                .prefix(prefix)
                .uploads(uploads)
                .build());
    }

    @Override
    public Class<ListMultipartUploadsOperation> getOperationType() {
        return ListMultipartUploadsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
