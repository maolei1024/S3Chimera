package win.ixuni.chimera.driver.mysql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 创建分片上传处理器
 */
@Slf4j
public class MysqlCreateMultipartUploadHandler
        extends AbstractMysqlHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    protected Mono<MultipartUpload> doHandle(CreateMultipartUploadOperation operation, MysqlDriverContext ctx) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // Use context.execute() to check if bucket exists
        return ctx.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return createUpload(ctx, operation);
                });
    }

    private Mono<MultipartUpload> createUpload(MysqlDriverContext ctx, CreateMultipartUploadOperation op) {
        String uploadId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        // Use correct field names: initiated_at and status
        String sql = "INSERT INTO t_multipart_upload (upload_id, bucket_name, object_key, content_type, initiated_at, status) "
                +
                "VALUES (?, ?, ?, ?, ?, 0)";

        return ctx.executeUpdate(ctx.getMetaConnectionPool(), sql,
                uploadId, op.getBucketName(), op.getKey(),
                op.getContentType() != null ? op.getContentType() : "application/octet-stream",
                LocalDateTime.now())
                .thenReturn(MultipartUpload.builder()
                        .uploadId(uploadId)
                        .bucketName(op.getBucketName())
                        .key(op.getKey())
                        .initiated(now)
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
