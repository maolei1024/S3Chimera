package win.ixuni.chimera.driver.postgresql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL ListObjects 处理器 (V1)
 */
@Slf4j
public class PostgresListObjectsHandler extends AbstractPostgresHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    protected Mono<ListObjectsResult> doHandle(ListObjectsOperation operation, PostgresDriverContext context) {
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        String marker = request.getMarker();
        int maxKeys = request.getMaxKeys() != null && request.getMaxKeys() > 0 ? request.getMaxKeys() : 1000;

        StringBuilder sqlBuilder = new StringBuilder(
                "SELECT object_key, size, etag, last_modified, storage_class FROM t_object WHERE bucket_name = $1");
        List<Object> params = new ArrayList<>();
        params.add(bucketName);

        int paramIndex = 2;
        if (!prefix.isEmpty()) {
            sqlBuilder.append(" AND object_key LIKE $").append(paramIndex++);
            params.add(prefix + "%");
        }
        if (marker != null && !marker.isEmpty()) {
            sqlBuilder.append(" AND object_key > $").append(paramIndex++);
            params.add(marker);
        }
        sqlBuilder.append(" ORDER BY object_key LIMIT $").append(paramIndex);
        params.add(maxKeys + 1);

        return context.executeQuery(context.getMetaConnectionPool(), sqlBuilder.toString(), params.toArray())
                .collectList()
                .map(rows -> buildResult(rows, bucketName, prefix, delimiter, maxKeys, context));
    }

    private ListObjectsResult buildResult(List<io.r2dbc.spi.Row> rows, String bucketName,
            String prefix, String delimiter, int maxKeys, PostgresDriverContext context) {
        List<S3Object> contents = new ArrayList<>();
        List<ListObjectsResult.CommonPrefix> commonPrefixes = new ArrayList<>();
        Set<String> prefixSet = new HashSet<>();
        boolean isTruncated = rows.size() > maxKeys;
        String nextMarker = null;

        int count = 0;
        for (io.r2dbc.spi.Row row : rows) {
            if (count >= maxKeys)
                break;
            String key = row.get("object_key", String.class);

            if (delimiter != null && !delimiter.isEmpty()) {
                String remaining = key.substring(prefix.length());
                int delimIndex = remaining.indexOf(delimiter);
                if (delimIndex >= 0) {
                    String commonPrefix = prefix + remaining.substring(0, delimIndex + delimiter.length());
                    if (prefixSet.add(commonPrefix)) {
                        commonPrefixes.add(ListObjectsResult.CommonPrefix.builder().prefix(commonPrefix).build());
                    }
                    continue;
                }
            }

            contents.add(S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(row.get("size", Long.class))
                    .etag(row.get("etag", String.class))
                    .lastModified(context.toInstant(row.get("last_modified", LocalDateTime.class)))
                    .storageClass(row.get("storage_class", String.class))
                    .build());
            nextMarker = key;
            count++;
        }

        return ListObjectsResult.builder()
                .bucketName(bucketName)
                .prefix(prefix)
                .delimiter(delimiter)
                .isTruncated(isTruncated)
                .contents(contents)
                .commonPrefixes(commonPrefixes)
                .nextMarker(isTruncated ? nextMarker : null)
                .build();
    }

    @Override
    public Class<ListObjectsOperation> getOperationType() {
        return ListObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
