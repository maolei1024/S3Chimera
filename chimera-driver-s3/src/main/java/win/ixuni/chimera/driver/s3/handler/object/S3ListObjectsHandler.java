package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理列出对象处理器 (V1)
 */
public class S3ListObjectsHandler implements OperationHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    public Mono<ListObjectsResult> handle(ListObjectsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        var request = operation.getRequest();

        var s3Request = ListObjectsRequest.builder()
                .bucket(request.getBucketName())
                .prefix(request.getPrefix())
                .delimiter(request.getDelimiter())
                .marker(request.getMarker())
                .maxKeys(request.getMaxKeys())
                .build();

        return Mono.fromFuture(() -> ctx.getS3Client().listObjects(s3Request))
                .map(response -> ListObjectsResult.builder()
                        .bucketName(response.name())
                        .prefix(response.prefix())
                        .delimiter(response.delimiter())
                        .isTruncated(response.isTruncated())
                        .nextMarker(response.nextMarker())
                        .contents(response.contents().stream()
                                .map(obj -> S3Object.builder()
                                        .bucketName(request.getBucketName())
                                        .key(obj.key())
                                        .size(obj.size())
                                        .etag(obj.eTag())
                                        .lastModified(obj.lastModified())
                                        .build())
                                .toList())
                        .commonPrefixes(response.commonPrefixes().stream()
                                .map(cp -> ListObjectsResult.CommonPrefix.builder()
                                        .prefix(cp.prefix())
                                        .build())
                                .toList())
                        .build());
    }

    @Override
    public Class<ListObjectsOperation> getOperationType() {
        return ListObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
