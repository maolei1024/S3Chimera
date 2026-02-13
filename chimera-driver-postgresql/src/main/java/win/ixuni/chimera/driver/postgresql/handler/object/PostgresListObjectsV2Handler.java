package win.ixuni.chimera.driver.postgresql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL ListObjectsV2 处理器
 */
@Slf4j
public class PostgresListObjectsV2Handler extends AbstractPostgresHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    protected Mono<ListObjectsV2Result> doHandle(ListObjectsV2Operation operation, PostgresDriverContext context) {
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        // 先检查 Bucket 是否存在
        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }
                    return listObjectsInternal(operation, context);
                });
    }

    private Mono<ListObjectsV2Result> listObjectsInternal(ListObjectsV2Operation operation,
            PostgresDriverContext context) {
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        String continuationToken = request.getContinuationToken();
        String startAfter = request.getStartAfter();
        int maxKeys = request.getMaxKeys() != null && request.getMaxKeys() > 0 ? request.getMaxKeys() : 1000;

        String lastKey = null;
        if (continuationToken != null && !continuationToken.isEmpty()) {
            try {
                lastKey = new String(Base64.getDecoder().decode(continuationToken));
            } catch (Exception e) {
                log.warn("无效的 continuationToken: {}", continuationToken);
            }
        }
        if (lastKey == null && startAfter != null) {
            lastKey = startAfter;
        }

        StringBuilder sqlBuilder = new StringBuilder(
                "SELECT object_key, size, etag, last_modified, storage_class FROM t_object WHERE bucket_name = $1");
        List<Object> params = new ArrayList<>();
        params.add(bucketName);

        int paramIndex = 2;
        if (!prefix.isEmpty()) {
            sqlBuilder.append(" AND object_key LIKE $").append(paramIndex++);
            params.add(prefix + "%");
        }
        if (lastKey != null && !lastKey.isEmpty()) {
            sqlBuilder.append(" AND object_key > $").append(paramIndex++);
            params.add(lastKey);
        }
        sqlBuilder.append(" ORDER BY object_key LIMIT $").append(paramIndex);
        params.add(maxKeys + 1);

        return context.executeQuery(context.getMetaConnectionPool(), sqlBuilder.toString(), params.toArray())
                .collectList()
                .map(rows -> buildResult(rows, bucketName, prefix, delimiter, maxKeys, context));
    }

    private ListObjectsV2Result buildResult(List<io.r2dbc.spi.Row> rows, String bucketName,
            String prefix, String delimiter, int maxKeys, PostgresDriverContext context) {
        List<S3Object> contents = new ArrayList<>();
        List<ListObjectsV2Result.CommonPrefix> commonPrefixes = new ArrayList<>();
        Set<String> prefixSet = new HashSet<>();
        boolean isTruncated = rows.size() > maxKeys;
        String nextContinuationToken = null;

        int count = 0;
        String lastKey = null;
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
                        commonPrefixes.add(ListObjectsV2Result.CommonPrefix.builder().prefix(commonPrefix).build());
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
            lastKey = key;
            count++;
        }

        if (isTruncated && lastKey != null) {
            nextContinuationToken = Base64.getEncoder().encodeToString(lastKey.getBytes());
        }

        return ListObjectsV2Result.builder()
                .name(bucketName)
                .prefix(prefix)
                .delimiter(delimiter)
                .maxKeys(maxKeys)
                .keyCount(contents.size())
                .isTruncated(isTruncated)
                .contents(contents)
                .commonPrefixes(commonPrefixes)
                .nextContinuationToken(nextContinuationToken)
                .build();
    }

    @Override
    public Class<ListObjectsV2Operation> getOperationType() {
        return ListObjectsV2Operation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
