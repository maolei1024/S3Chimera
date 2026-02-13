package win.ixuni.chimera.driver.postgresql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.multipart.ListMultipartUploadsOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL 列出分片上传会话处理器
 */
@Slf4j
public class PostgresListMultipartUploadsHandler
        extends AbstractPostgresHandler<ListMultipartUploadsOperation, ListMultipartUploadsResult> {

    @Override
    protected Mono<ListMultipartUploadsResult> doHandle(ListMultipartUploadsOperation operation,
            PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String prefix = operation.getPrefix();
        int maxUploads = 1000;

        StringBuilder sqlBuilder = new StringBuilder(
                "SELECT upload_id, object_key, initiated_at FROM t_multipart_upload WHERE bucket_name = $1");
        List<Object> params = new ArrayList<>();
        params.add(bucketName);

        int paramIndex = 2;
        if (prefix != null && !prefix.isEmpty()) {
            sqlBuilder.append(" AND object_key LIKE $").append(paramIndex++);
            params.add(prefix + "%");
        }
        sqlBuilder.append(" ORDER BY object_key, upload_id LIMIT $").append(paramIndex);
        params.add(maxUploads + 1);

        return context.executeQuery(context.getMetaConnectionPool(), sqlBuilder.toString(), params.toArray())
                .collectList()
                .map(rows -> {
                    boolean isTruncated = rows.size() > maxUploads;
                    String nextKeyMarker = null;
                    String nextUploadIdMarker = null;

                    List<MultipartUpload> uploads = new ArrayList<>();
                    int count = 0;
                    for (var row : rows) {
                        if (count >= maxUploads)
                            break;
                        String uploadId = row.get("upload_id", String.class);
                        String objectKey = row.get("object_key", String.class);
                        uploads.add(MultipartUpload.builder()
                                .uploadId(uploadId)
                                .bucketName(bucketName)
                                .key(objectKey)
                                .initiated(context.toInstant(row.get("initiated_at", LocalDateTime.class)))
                                .build());
                        nextKeyMarker = objectKey;
                        nextUploadIdMarker = uploadId;
                        count++;
                    }

                    return ListMultipartUploadsResult.builder()
                            .bucketName(bucketName)
                            .prefix(prefix)
                            .isTruncated(isTruncated)
                            .uploads(uploads)
                            .nextKeyMarker(isTruncated ? nextKeyMarker : null)
                            .nextUploadIdMarker(isTruncated ? nextUploadIdMarker : null)
                            .build();
                })
                .doOnNext(result -> log.debug("ListMultipartUploads: bucket={}, 共 {} 个上传会话",
                        bucketName, result.getUploads().size()));
    }

    @Override
    public Class<ListMultipartUploadsOperation> getOperationType() {
        return ListMultipartUploadsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ, Capability.MULTIPART_UPLOAD);
    }
}
