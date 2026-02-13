package win.ixuni.chimera.driver.sftp.handler.multipart;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * SFTP create multipart upload handler
 * <p>
 * Create part storage structure in local temporary directory
 */
public class SftpCreateMultipartUploadHandler
        implements OperationHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    public Mono<MultipartUpload> handle(CreateMultipartUploadOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            // Check if remote bucket exists
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);
                try {
                    var attrs = sftpClient.stat(bucketPath);
                    if (!attrs.isDirectory()) {
                        throw new IllegalArgumentException("Bucket does not exist: " + bucketName);
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException("Bucket does not exist: " + bucketName);
                }

                String uploadId = UUID.randomUUID().toString();
                Instant now = Instant.now();

                // Create local temporary directory to store parts
                Files.createDirectories(ctx.getMultipartTempPath(uploadId));

                // 记录上传状态
                var state = SftpDriverContext.MultipartState.builder()
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
            }
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
