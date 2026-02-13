package win.ixuni.chimera.driver.postgresql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * PostgreSQL 创建分片上传处理器
 */
@Slf4j
public class PostgresCreateMultipartUploadHandler
        extends AbstractPostgresHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    protected Mono<MultipartUpload> doHandle(CreateMultipartUploadOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }

                    String uploadId = UUID.randomUUID().toString().replace("-", "");
                    Instant now = Instant.now();

                    String sql = "INSERT INTO t_multipart_upload " +
                            "(upload_id, bucket_name, object_key, content_type, initiated_at) " +
                            "VALUES ($1, $2, $3, $4, $5)";

                    return context.executeUpdate(context.getMetaConnectionPool(), sql,
                            uploadId, bucketName, key, operation.getContentType(), LocalDateTime.now())
                            .thenReturn(MultipartUpload.builder()
                                    .uploadId(uploadId)
                                    .bucketName(bucketName)
                                    .key(key)
                                    .initiated(now)
                                    .build());
                })
                .doOnNext(upload -> log.debug("CreateMultipartUpload: bucket={}, key={}, uploadId={}",
                        bucketName, key, upload.getUploadId()));
    }

    @Override
    public Class<CreateMultipartUploadOperation> getOperationType() {
        return CreateMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
    }
}
