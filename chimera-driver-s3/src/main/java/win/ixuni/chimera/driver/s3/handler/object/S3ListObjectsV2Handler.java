package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理列出对象处理器 (V2)
 */
public class S3ListObjectsV2Handler implements OperationHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    public Mono<ListObjectsV2Result> handle(ListObjectsV2Operation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        var request = operation.getRequest();

        var s3Request = ListObjectsV2Request.builder()
                .bucket(request.getBucketName())
                .prefix(request.getPrefix())
                .delimiter(request.getDelimiter())
                .startAfter(request.getStartAfter())
                .continuationToken(request.getContinuationToken())
                .maxKeys(request.getMaxKeys())
                .build();

        return Mono.fromFuture(() -> ctx.getS3Client().listObjectsV2(s3Request))
                .map(response -> ListObjectsV2Result.builder()
                        .name(response.name())
                        .prefix(response.prefix())
                        .delimiter(response.delimiter())
                        .isTruncated(response.isTruncated())
                        .keyCount(response.keyCount())
                        .maxKeys(response.maxKeys())
                        .startAfter(response.startAfter())
                        .continuationToken(response.continuationToken())
                        .nextContinuationToken(response.nextContinuationToken())
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
                                .map(cp -> ListObjectsV2Result.CommonPrefix.builder()
                                        .prefix(cp.prefix())
                                        .build())
                                .toList())
                        .build());
    }

    @Override
    public Class<ListObjectsV2Operation> getOperationType() {
        return ListObjectsV2Operation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
