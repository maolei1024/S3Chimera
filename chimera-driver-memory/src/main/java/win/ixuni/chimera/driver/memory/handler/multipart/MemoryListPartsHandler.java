package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.util.Comparator;
import java.util.List;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 列出分片处理器
 */
public class MemoryListPartsHandler implements OperationHandler<ListPartsOperation, ListPartsResult> {

    @Override
    public Mono<ListPartsResult> handle(ListPartsOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String uploadId = operation.getRequest().getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new win.ixuni.chimera.core.exception.NoSuchUploadException(uploadId));
        }

        List<UploadPart> parts = state.getParts().entrySet().stream()
                .map(e -> UploadPart.builder()
                        .partNumber(e.getKey())
                        .etag(e.getValue().getEtag())
                        .size((long) e.getValue().getData().length)
                        .lastModified(e.getValue().getLastModified())
                        .build())
                .sorted(Comparator.comparingInt(UploadPart::getPartNumber))
                .toList();

        return Mono.just(ListPartsResult.builder()
                .bucketName(operation.getRequest().getBucketName())
                .key(operation.getRequest().getKey())
                .uploadId(uploadId)
                .parts(parts)
                .build());
    }

    @Override
    public Class<ListPartsOperation> getOperationType() {
        return ListPartsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
