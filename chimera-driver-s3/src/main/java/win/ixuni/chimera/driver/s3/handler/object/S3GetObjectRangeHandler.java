package win.ixuni.chimera.driver.s3.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理获取对象范围处理器
 * <p>
 * Uses streaming responses to avoid loading entire content into memory
 */
public class S3GetObjectRangeHandler implements OperationHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectRangeOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 构建 Range 请求头
        String range = "bytes=" + operation.getRangeStart() + "-";
        if (operation.getRangeEnd() != null) {
            range += operation.getRangeEnd();
        }

        String finalRange = range;
        return Mono.fromFuture(() -> ctx.getS3Client().getObject(
                GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .range(finalRange)
                        .build(),
                AsyncResponseTransformer.toPublisher()))
                .map(publisher -> {
                    GetObjectResponse response = publisher.response();
                    S3Object metadata = S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size(response.contentLength())
                            .etag(response.eTag())
                            .lastModified(response.lastModified())
                            .contentType(response.contentType())
                            .build();

                    // 流式转发
                    Flux<ByteBuffer> content = Flux.from(publisher);
                    return S3ObjectData.builder()
                            .metadata(metadata)
                            .content(content)
                            .build();
                });
    }

    @Override
    public Class<GetObjectRangeOperation> getOperationType() {
        return GetObjectRangeOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ, Capability.RANGE_READ);
    }
}
