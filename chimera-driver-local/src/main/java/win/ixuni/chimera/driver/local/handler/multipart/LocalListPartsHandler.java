package win.ixuni.chimera.driver.local.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Local 列出分片处理器
 */
public class LocalListPartsHandler implements OperationHandler<ListPartsOperation, ListPartsResult> {

    @Override
    public Mono<ListPartsResult> handle(ListPartsOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String uploadId = operation.getRequest().getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new win.ixuni.chimera.core.exception.NoSuchUploadException(uploadId));
        }

        List<UploadPart> parts = state.getParts().values().stream()
                .map(p -> UploadPart.builder()
                        .partNumber(p.getPartNumber())
                        .etag(p.getEtag())
                        .size(p.getSize())
                        .lastModified(p.getLastModified())
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
