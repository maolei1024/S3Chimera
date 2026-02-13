package win.ixuni.chimera.driver.mysql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;

/**
 * MySQL 列出对象处理器 (V2)
 */
@Slf4j
public class MysqlListObjectsV2Handler extends AbstractMysqlHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    protected Mono<ListObjectsV2Result> doHandle(ListObjectsV2Operation operation, MysqlDriverContext ctx) {
        var request = operation.getRequest();
        String bucket = request.getBucketName();

        // Use context.execute() to check if bucket exists
        return ctx.execute(new BucketExistsOperation(bucket))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucket));
                    }
                    return listObjectsFromTable(ctx, request);
                });
    }

    private Mono<ListObjectsV2Result> listObjectsFromTable(MysqlDriverContext ctx,
            win.ixuni.chimera.core.model.ListObjectsRequest request) {
        String bucket = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        int maxKeys = request.getMaxKeys();
        String continuationToken = request.getContinuationToken();
        String startAfter = request.getStartAfter();

        String startKey = null;
        if (continuationToken != null && !continuationToken.isEmpty()) {
            startKey = continuationToken;
        } else if (startAfter != null && !startAfter.isEmpty()) {
            startKey = startAfter;
        }

        final String effectiveStartKey = startKey;
        int batchSize = Math.max(maxKeys * 2, 2000);

        return collectEntries(ctx, bucket, prefix, delimiter, maxKeys, effectiveStartKey, batchSize)
                .map(result -> ListObjectsV2Result.builder()
                        .name(bucket)
                        .prefix(prefix.isEmpty() ? null : prefix)
                        .delimiter(delimiter)
                        .maxKeys(maxKeys)
                        .keyCount(result.contents.size() + result.commonPrefixes.size())
                        .isTruncated(result.isTruncated)
                        .continuationToken(continuationToken)
                        .nextContinuationToken(result.nextToken)
                        .contents(result.contents)
                        .commonPrefixes(result.commonPrefixes.isEmpty() ? null
                                : result.commonPrefixes.stream()
                                        .map(p -> ListObjectsV2Result.CommonPrefix.builder().prefix(p).build())
                                        .toList())
                        .build());
    }

    private Mono<ListResult> collectEntries(MysqlDriverContext ctx, String bucket, String prefix,
            String delimiter, int maxKeys, String startKey, int batchSize) {
        Set<String> commonPrefixes = new TreeSet<>();
        List<S3Object> contents = new ArrayList<>();

        return queryObjects(ctx, bucket, prefix, startKey, batchSize + 1)
                .collectList()
                .map(objects -> {
                    if (objects.isEmpty()) {
                        return new ListResult(commonPrefixes, contents, null, false);
                    }

                    String lastKey = null;
                    boolean reachedMax = false;
                    int processed = 0;

                    for (S3Object obj : objects) {
                        if (processed >= batchSize)
                            break;

                        String keyAfterPrefix = obj.getKey().substring(prefix.length());
                        int delimIdx = delimiter != null ? keyAfterPrefix.indexOf(delimiter) : -1;

                        if (delimIdx > 0) {
                            String cp = prefix + keyAfterPrefix.substring(0, delimIdx + 1);
                            if (!commonPrefixes.contains(cp)) {
                                if (contents.size() + commonPrefixes.size() >= maxKeys) {
                                    reachedMax = true;
                                    break;
                                }
                                commonPrefixes.add(cp);
                            }
                        } else {
                            if (contents.size() + commonPrefixes.size() >= maxKeys) {
                                reachedMax = true;
                                break;
                            }
                            contents.add(obj);
                        }
                        lastKey = obj.getKey();
                        processed++;
                    }

                    boolean isTruncated = reachedMax || objects.size() > batchSize;
                    return new ListResult(commonPrefixes, contents, isTruncated ? lastKey : null, isTruncated);
                });
    }

    private Flux<S3Object> queryObjects(MysqlDriverContext ctx, String bucket, String prefix,
            String startKey, int limit) {
        String sql;
        Object[] params;

        if (prefix.isEmpty() && (startKey == null || startKey.isEmpty())) {
            sql = "SELECT bucket_name, object_key, size, etag, content_type, last_modified " +
                    "FROM t_object WHERE bucket_name = ? ORDER BY object_key LIMIT ?";
            params = new Object[] { bucket, limit };
        } else if (prefix.isEmpty()) {
            sql = "SELECT bucket_name, object_key, size, etag, content_type, last_modified " +
                    "FROM t_object WHERE bucket_name = ? AND object_key > ? ORDER BY object_key LIMIT ?";
            params = new Object[] { bucket, startKey, limit };
        } else if (startKey == null || startKey.isEmpty()) {
            sql = "SELECT bucket_name, object_key, size, etag, content_type, last_modified " +
                    "FROM t_object WHERE bucket_name = ? AND object_key LIKE ? ORDER BY object_key LIMIT ?";
            params = new Object[] { bucket, prefix + "%", limit };
        } else {
            sql = "SELECT bucket_name, object_key, size, etag, content_type, last_modified " +
                    "FROM t_object WHERE bucket_name = ? AND object_key LIKE ? AND object_key > ? " +
                    "ORDER BY object_key LIMIT ?";
            params = new Object[] { bucket, prefix + "%", startKey, limit };
        }

        return ctx.executeQueryMapped(ctx.getMetaConnectionPool(), sql,
                row -> S3Object.builder()
                        .bucketName(row.get("bucket_name", String.class))
                        .key(row.get("object_key", String.class))
                        .size(row.get("size", Long.class))
                        .etag(row.get("etag", String.class))
                        .contentType(row.get("content_type", String.class))
                        .lastModified(ctx.toInstant(row.get("last_modified", LocalDateTime.class)))
                        .storageClass("STANDARD")
                        .build(),
                params);
    }

    private static class ListResult {
        final Set<String> commonPrefixes;
        final List<S3Object> contents;
        final String nextToken;
        final boolean isTruncated;

        ListResult(Set<String> cp, List<S3Object> c, String next, boolean truncated) {
            this.commonPrefixes = cp;
            this.contents = c;
            this.nextToken = next;
            this.isTruncated = truncated;
        }
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
