package win.ixuni.chimera.driver.local.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.nio.file.Files;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * Local 创建分片上传处理器
 */
public class LocalCreateMultipartUploadHandler
        implements OperationHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    public Mono<MultipartUpload> handle(CreateMultipartUploadOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            // 检查 bucket 是否存在
            if (!Files.exists(ctx.getBucketPath(bucketName))) {
                throw new BucketNotFoundException(bucketName);
            }

            String uploadId = UUID.randomUUID().toString();
            Instant now = Instant.now();

            // 创建临时目录存储分片
            Files.createDirectories(ctx.getMultipartPath(bucketName, uploadId));

            // 记录上传状态
            var state = LocalDriverContext.MultipartState.builder()
                    .uploadId(uploadId)
                    .bucketName(bucketName)
                    .key(key)
                    .contentType(operation.getContentType())
                    .metadata(operation.getMetadata())
                    .initiated(now)
                    .build();

            ctx.getMultipartUploads().put(uploadId, state);

            // 持久化分片状态到磁盘
            ctx.persistMultipartState(state);

            return MultipartUpload.builder()
                    .uploadId(uploadId)
                    .bucketName(bucketName)
                    .key(key)
                    .initiated(now)
                    .build();
        });
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
