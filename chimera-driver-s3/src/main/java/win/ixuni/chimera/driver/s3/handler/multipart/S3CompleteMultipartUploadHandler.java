package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理完成分片上传处理器
 * <p>
 * 直接透传到后端 S3 服务
 */
public class S3CompleteMultipartUploadHandler
        implements OperationHandler<CompleteMultipartUploadOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CompleteMultipartUploadOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        // 转换 parts
        var completedParts = operation.getParts().stream()
                .map(p -> CompletedPart.builder()
                        .partNumber(p.getPartNumber())
                        .eTag(p.getEtag())
                        .build())
                .toList();

        var request = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build())
                .build();

        return Mono.fromFuture(() -> ctx.getS3Client().completeMultipartUpload(request))
                .map(response -> S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .etag(response.eTag())
                        .lastModified(Instant.now())
                        .storageClass("STANDARD")
                        .build());
    }

    @Override
    public Class<CompleteMultipartUploadOperation> getOperationType() {
        return CompleteMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
