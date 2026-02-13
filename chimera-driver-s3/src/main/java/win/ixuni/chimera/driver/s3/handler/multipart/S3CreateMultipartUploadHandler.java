package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理创建分片上传处理器
 * <p>
 * 直接透传到后端 S3 服务
 */
public class S3CreateMultipartUploadHandler
        implements OperationHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    public Mono<MultipartUpload> handle(CreateMultipartUploadOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        var requestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key);

        if (operation.getContentType() != null) {
            requestBuilder.contentType(operation.getContentType());
        }
        if (operation.getMetadata() != null && !operation.getMetadata().isEmpty()) {
            requestBuilder.metadata(operation.getMetadata());
        }

        return Mono.fromFuture(() -> ctx.getS3Client().createMultipartUpload(requestBuilder.build()))
                .map(response -> MultipartUpload.builder()
                        .uploadId(response.uploadId())
                        .bucketName(bucketName)
                        .key(key)
                        .initiated(Instant.now())
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
