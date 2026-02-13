package win.ixuni.chimera.driver.mysql.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.multipart.ListMultipartUploadsOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.LocalDateTime;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 列出正在进行的分片上传处理器
 */
@Slf4j
public class MysqlListMultipartUploadsHandler
                extends AbstractMysqlHandler<ListMultipartUploadsOperation, ListMultipartUploadsResult> {

        @Override
        protected Mono<ListMultipartUploadsResult> doHandle(ListMultipartUploadsOperation operation,
                        MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String prefix = operation.getPrefix();

                // Use correct field name initiated_at, query only status=0 (in-progress) uploads
                String sql = "SELECT upload_id, bucket_name, object_key, initiated_at FROM t_multipart_upload " +
                                "WHERE bucket_name = ? AND status = 0" +
                                (prefix != null ? " AND object_key LIKE ?" : "") +
                                " ORDER BY initiated_at";

                Object[] params = prefix != null ? new Object[] { bucketName, prefix + "%" }
                                : new Object[] { bucketName };

                return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                                row -> MultipartUpload.builder()
                                                .uploadId(row.get("upload_id", String.class))
                                                .bucketName(row.get("bucket_name", String.class))
                                                .key(row.get("object_key", String.class))
                                                .initiated(ctx.toInstant(row.get("initiated_at", LocalDateTime.class)))
                                                .build(),
                                params)
                                .collectList()
                                .map(uploads -> ListMultipartUploadsResult.builder()
                                                .bucketName(bucketName)
                                                .prefix(prefix)
                                                .uploads(uploads)
                                                .isTruncated(false)
                                                .build());
        }

        @Override
        public Class<ListMultipartUploadsOperation> getOperationType() {
                return ListMultipartUploadsOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.MULTIPART_UPLOAD);
        }
}
