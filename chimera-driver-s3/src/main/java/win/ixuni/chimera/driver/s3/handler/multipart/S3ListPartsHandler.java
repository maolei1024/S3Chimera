package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理列出分片处理器
 * <p>
 * 直接透传到后端 S3 服务
 */
public class S3ListPartsHandler implements OperationHandler<ListPartsOperation, ListPartsResult> {

    @Override
    public Mono<ListPartsResult> handle(ListPartsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        var req = operation.getRequest();

        var requestBuilder = ListPartsRequest.builder()
                .bucket(req.getBucketName())
                .key(req.getKey())
                .uploadId(req.getUploadId());

        if (req.getMaxParts() != null) {
            requestBuilder.maxParts(req.getMaxParts());
        }
        if (req.getPartNumberMarker() != null) {
            requestBuilder.partNumberMarker(req.getPartNumberMarker());
        }

        return Mono.fromFuture(() -> ctx.getS3Client().listParts(requestBuilder.build()))
                .map(response -> {
                    var parts = response.parts().stream()
                            .map(p -> UploadPart.builder()
                                    .partNumber(p.partNumber())
                                    .etag(p.eTag())
                                    .size(p.size())
                                    .lastModified(p.lastModified())
                                    .build())
                            .toList();

                    return ListPartsResult.builder()
                            .bucketName(req.getBucketName())
                            .key(req.getKey())
                            .uploadId(req.getUploadId())
                            .parts(parts)
                            .maxParts(response.maxParts())
                            .isTruncated(response.isTruncated())
                            .nextPartNumberMarker(response.nextPartNumberMarker())
                            .build();
                });
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
