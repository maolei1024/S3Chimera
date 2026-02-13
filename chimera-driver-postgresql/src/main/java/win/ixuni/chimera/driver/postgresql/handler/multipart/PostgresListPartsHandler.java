package win.ixuni.chimera.driver.postgresql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListPartsRequest;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL 列出分片处理器
 */
@Slf4j
public class PostgresListPartsHandler extends AbstractPostgresHandler<ListPartsOperation, ListPartsResult> {

    @Override
    protected Mono<ListPartsResult> doHandle(ListPartsOperation operation, PostgresDriverContext context) {
        ListPartsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();
        String key = request.getKey();
        String uploadId = request.getUploadId();
        int maxParts = request.getMaxParts() != null && request.getMaxParts() > 0 ? request.getMaxParts() : 1000;
        int partNumberMarker = request.getPartNumberMarker() != null ? request.getPartNumberMarker() : 0;

        String checkSql = "SELECT count(1) FROM t_multipart_upload WHERE upload_id = $1";
        return context.executeQuery(context.getMetaConnectionPool(), checkSql, uploadId)
                .next()
                .flatMap(row -> {
                    Long count = row.get(0, Long.class);
                    if (count == null || count == 0) {
                        return Mono.error(new win.ixuni.chimera.core.exception.NoSuchUploadException(uploadId));
                    }

                    String sql = "SELECT part_number, size, etag, uploaded_at FROM t_multipart_part " +
                            "WHERE upload_id = $1 AND part_number > $2 ORDER BY part_number LIMIT $3";

                    return context
                            .executeQuery(context.getMetaConnectionPool(), sql, uploadId, partNumberMarker,
                                    maxParts + 1)
                            .collectList();
                })
                .map(rows -> {
                    boolean isTruncated = rows.size() > maxParts;
                    int nextPartNumberMarker = 0;

                    List<UploadPart> parts = new ArrayList<>();
                    int count = 0;
                    for (var row : rows) {
                        if (count >= maxParts)
                            break;
                        int partNumber = row.get("part_number", Integer.class);
                        parts.add(UploadPart.builder()
                                .partNumber(partNumber)
                                .size(row.get("size", Long.class))
                                .etag(row.get("etag", String.class))
                                .lastModified(context.toInstant(row.get("uploaded_at", LocalDateTime.class)))
                                .build());
                        nextPartNumberMarker = partNumber;
                        count++;
                    }

                    return ListPartsResult.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .maxParts(maxParts)
                            .isTruncated(isTruncated)
                            .parts(parts)
                            .nextPartNumberMarker(isTruncated ? nextPartNumberMarker : 0)
                            .build();
                })
                .doOnNext(result -> log.debug("ListParts: uploadId={}, 共 {} 个 part",
                        uploadId, result.getParts().size()));
    }

    @Override
    public Class<ListPartsOperation> getOperationType() {
        return ListPartsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ, Capability.MULTIPART_UPLOAD);
    }
}
