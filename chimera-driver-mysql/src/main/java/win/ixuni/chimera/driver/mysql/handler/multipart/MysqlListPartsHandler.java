package win.ixuni.chimera.driver.mysql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListPartsRequest;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.LocalDateTime;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 列出分片处理器
 */
@Slf4j
public class MysqlListPartsHandler extends AbstractMysqlHandler<ListPartsOperation, ListPartsResult> {

        @Override
        protected Mono<ListPartsResult> doHandle(ListPartsOperation operation, MysqlDriverContext ctx) {
                ListPartsRequest request = operation.getRequest();

                // Use correct field name uploaded_at
                // 先检查 uploadId 是否存在
                String checkSql = "SELECT count(1) as cnt FROM t_multipart_upload WHERE upload_id = ?";
                return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), checkSql,
                                row -> row.get("cnt", Long.class), request.getUploadId())
                                .next()
                                .flatMap(count -> {
                                        if (count == null || count == 0) {
                                                return Mono.error(
                                                                new win.ixuni.chimera.core.exception.NoSuchUploadException(
                                                                                request.getUploadId()));
                                        }
                                        // 如果存在，继续查询分片
                                        String sql = "SELECT part_number, etag, size, uploaded_at FROM t_multipart_part "
                                                        +
                                                        "WHERE upload_id = ? ORDER BY part_number";
                                        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                                                        partRow -> UploadPart.builder()
                                                                        .partNumber(partRow.get("part_number",
                                                                                        Integer.class))
                                                                        .etag(partRow.get("etag", String.class))
                                                                        .size(partRow.get("size", Long.class))
                                                                        .lastModified(ctx.toInstant(partRow.get(
                                                                                        "uploaded_at",
                                                                                        LocalDateTime.class)))
                                                                        .build(),
                                                        request.getUploadId())
                                                        .collectList();
                                })
                                .map(parts -> ListPartsResult.builder()
                                                .bucketName(request.getBucketName())
                                                .key(request.getKey())
                                                .uploadId(request.getUploadId())
                                                .parts(parts)
                                                .isTruncated(false)
                                                .build());
        }

        @Override
        public Class<ListPartsOperation> getOperationType() {
                return ListPartsOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.MULTIPART_UPLOAD);
        }
}
