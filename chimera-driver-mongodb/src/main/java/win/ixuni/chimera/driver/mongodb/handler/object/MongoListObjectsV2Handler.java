package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB ListObjectsV2 处理器
 */
@Slf4j
public class MongoListObjectsV2Handler extends AbstractMongoHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    protected Mono<ListObjectsV2Result> doHandle(ListObjectsV2Operation operation, MongoDriverContext context) {
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
            MongoDriverContext context) {
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

        List<org.bson.conversions.Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("bucketName", bucketName));
        if (!prefix.isEmpty()) {
            filters.add(Filters.regex("objectKey", "^" + escapeRegex(prefix)));
        }
        if (lastKey != null && !lastKey.isEmpty()) {
            filters.add(Filters.gt("objectKey", lastKey));
        }

        org.bson.conversions.Bson filter = Filters.and(filters);

        return Flux.from(context.getObjectCollection()
                .find(filter)
                .sort(Sorts.ascending("objectKey"))
                .limit(maxKeys + 1))
                .collectList()
                .map(docs -> buildResult(docs, bucketName, prefix, delimiter, maxKeys, context));
    }

    private ListObjectsV2Result buildResult(List<Document> docs, String bucketName,
            String prefix, String delimiter, int maxKeys, MongoDriverContext context) {
        List<S3Object> contents = new ArrayList<>();
        List<ListObjectsV2Result.CommonPrefix> commonPrefixes = new ArrayList<>();
        java.util.Set<String> prefixSet = new java.util.HashSet<>();
        boolean isTruncated = docs.size() > maxKeys;
        String nextContinuationToken = null;

        int count = 0;
        String lastKey = null;
        for (Document doc : docs) {
            if (count >= maxKeys)
                break;
            String key = doc.getString("objectKey");

            if (delimiter != null && !delimiter.isEmpty()) {
                String remaining = key.substring(prefix.length());
                int delimIndex = remaining.indexOf(delimiter);
                if (delimIndex >= 0) {
                    String commonPrefix = prefix + remaining.substring(0, delimIndex + delimiter.length());
                    if (prefixSet.add(commonPrefix)) {
                        commonPrefixes.add(ListObjectsV2Result.CommonPrefix.builder().prefix(commonPrefix).build());
                        count++; // CommonPrefix 也计入 maxKeys
                    }
                    continue;
                }
            }

            contents.add(S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(doc.getLong("size"))
                    .etag(doc.getString("etag"))
                    .lastModified(context.toInstant(doc.getDate("lastModified")))
                    .storageClass(doc.getString("storageClass"))
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

    private String escapeRegex(String str) {
        return str.replaceAll("([\\\\\\^\\$\\.\\|\\?\\*\\+\\(\\)\\[\\]\\{\\}])", "\\\\$1");
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
