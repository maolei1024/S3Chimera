package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理中止分片上传处理器
 * <p>
 * 直接透传到后端 S3 服务
 */
public class S3AbortMultipartUploadHandler
        implements OperationHandler<AbortMultipartUploadOperation, Void> {

    @Override
    public Mono<Void> handle(AbortMultipartUploadOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;

        var request = AbortMultipartUploadRequest.builder()
                .bucket(operation.getBucketName())
                .key(operation.getKey())
                .uploadId(operation.getUploadId())
                .build();

        return Mono.fromFuture(() -> ctx.getS3Client().abortMultipartUpload(request))
                .then();
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
