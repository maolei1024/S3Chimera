package win.ixuni.chimera.driver.webdav.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.nio.file.Files;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * WebDAV 创建分片上传处理器
 * <p>
 * Create part storage structure in local temporary directory
 */
public class WebDavCreateMultipartUploadHandler
        implements OperationHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    public Mono<MultipartUpload> handle(CreateMultipartUploadOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            // Check if remote bucket exists
            String bucketUrl = ctx.getBucketUrl(bucketName);
            if (!ctx.getSardine().exists(bucketUrl)) {
                throw new IllegalArgumentException("Bucket does not exist: " + bucketName);
            }

            String uploadId = UUID.randomUUID().toString();
            Instant now = Instant.now();

            // Create local temporary directory to store parts
            Files.createDirectories(ctx.getMultipartTempPath(uploadId));

            // 记录上传状态
            var state = WebDavDriverContext.MultipartState.builder()
                    .uploadId(uploadId)
                    .bucketName(bucketName)
                    .key(key)
                    .contentType(operation.getContentType())
                    .metadata(operation.getMetadata())
                    .initiated(now)
                    .build();

            ctx.getMultipartUploads().put(uploadId, state);

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
