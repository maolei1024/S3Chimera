package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理上传对象处理器
 * <p>
 * Note: current implementation buffers all content to memory before sending to backend S3.
 * This is because the AWS SDK's {@code AsyncRequestBody.fromPublisher()} requires content-length,
 * Chunked uploads without content-length are not supported by all S3-compatible backends.
 * For very large files, multipart upload should be used.
 */
public class S3PutObjectHandler implements OperationHandler<PutObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(PutObjectOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 收集内容
        return operation.getContent()
                .reduce(new ByteArrayOutputStream(), (baos, buffer) -> {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    baos.write(bytes, 0, bytes.length);
                    return baos;
                })
                .flatMap(baos -> {
                    byte[] data = baos.toByteArray();

                    var requestBuilder = PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .contentType(operation.getContentType())
                            .contentLength((long) data.length);

                    if (operation.getMetadata() != null && !operation.getMetadata().isEmpty()) {
                        requestBuilder.metadata(operation.getMetadata());
                    }

                    return Mono.fromFuture(() -> ctx.getS3Client().putObject(
                            requestBuilder.build(),
                            AsyncRequestBody.fromBytes(data)))
                            .map(response -> S3Object.builder()
                                    .bucketName(bucketName)
                                    .key(key)
                                    .size((long) data.length)
                                    .etag(response.eTag())
                                    .lastModified(Instant.now())
                                    .contentType(operation.getContentType())
                                    .userMetadata(operation.getMetadata())
                                    .build());
                });
    }

    @Override
    public Class<PutObjectOperation> getOperationType() {
        return PutObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
