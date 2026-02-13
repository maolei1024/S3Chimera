package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.ListMultipartUploadsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理列出分片上传处理器
 * <p>
 * 直接透传到后端 S3 服务
 */
public class S3ListMultipartUploadsHandler
        implements OperationHandler<ListMultipartUploadsOperation, ListMultipartUploadsResult> {

    @Override
    public Mono<ListMultipartUploadsResult> handle(ListMultipartUploadsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();

        var requestBuilder = ListMultipartUploadsRequest.builder()
                .bucket(bucketName);

        if (operation.getPrefix() != null) {
            requestBuilder.prefix(operation.getPrefix());
        }

        return Mono.fromFuture(() -> ctx.getS3Client().listMultipartUploads(requestBuilder.build()))
                .map(response -> {
                    var uploads = response.uploads().stream()
                            .map(u -> MultipartUpload.builder()
                                    .uploadId(u.uploadId())
                                    .bucketName(bucketName)
                                    .key(u.key())
                                    .initiated(u.initiated())
                                    .build())
                            .toList();

                    return ListMultipartUploadsResult.builder()
                            .bucketName(bucketName)
                            .prefix(operation.getPrefix())
                            .uploads(uploads)
                            .isTruncated(response.isTruncated())
                            .nextKeyMarker(response.nextKeyMarker())
                            .nextUploadIdMarker(response.nextUploadIdMarker())
                            .build();
                });
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
