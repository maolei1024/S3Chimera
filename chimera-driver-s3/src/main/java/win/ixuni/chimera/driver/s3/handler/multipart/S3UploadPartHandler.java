package win.ixuni.chimera.driver.s3.handler.multipart;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理上传分片处理器
 * <p>
 * 直接透传到后端 S3 服务。
 * <p>
 * Note: current implementation buffers part content to memory before sending.
 * 单个分片通常为 5-100MB，在可接受范围内。
 */
public class S3UploadPartHandler implements OperationHandler<UploadPartOperation, UploadPart> {

    @Override
    public Mono<UploadPart> handle(UploadPartOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        // 收集数据
        return operation.getContent()
                .reduce(new ByteArrayOutputStream(), (baos, buffer) -> {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    baos.write(bytes, 0, bytes.length);
                    return baos;
                })
                .flatMap(baos -> {
                    byte[] data = baos.toByteArray();

                    var request = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) data.length)
                            .build();

                    return Mono.fromFuture(() -> ctx.getS3Client().uploadPart(
                            request, AsyncRequestBody.fromBytes(data)))
                            .map(response -> UploadPart.builder()
                                    .partNumber(partNumber)
                                    .etag(response.eTag())
                                    .size((long) data.length)
                                    .lastModified(Instant.now())
                                    .build());
                });
    }

    @Override
    public Class<UploadPartOperation> getOperationType() {
        return UploadPartOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
