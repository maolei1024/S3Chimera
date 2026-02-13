package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB ListObjects 处理器 (V1)
 */
@Slf4j
public class MongoListObjectsHandler extends AbstractMongoHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    protected Mono<ListObjectsResult> doHandle(ListObjectsOperation operation, MongoDriverContext context) {
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        String marker = request.getMarker();
        int maxKeys = request.getMaxKeys() != null && request.getMaxKeys() > 0 ? request.getMaxKeys() : 1000;

        List<org.bson.conversions.Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("bucketName", bucketName));
        if (!prefix.isEmpty()) {
            filters.add(Filters.regex("objectKey", "^" + escapeRegex(prefix)));
        }
        if (marker != null && !marker.isEmpty()) {
            filters.add(Filters.gt("objectKey", marker));
        }

        org.bson.conversions.Bson filter = Filters.and(filters);

        return Flux.from(context.getObjectCollection()
                .find(filter)
                .sort(Sorts.ascending("objectKey"))
                .limit(maxKeys + 1))
                .collectList()
                .map(docs -> buildResult(docs, bucketName, prefix, delimiter, maxKeys, context));
    }

    private ListObjectsResult buildResult(List<Document> docs, String bucketName,
            String prefix, String delimiter, int maxKeys, MongoDriverContext context) {
        List<S3Object> contents = new ArrayList<>();
        List<ListObjectsResult.CommonPrefix> commonPrefixes = new ArrayList<>();
        java.util.Set<String> prefixSet = new java.util.HashSet<>();
        boolean isTruncated = docs.size() > maxKeys;
        String nextMarker = null;

        int count = 0;
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
                        commonPrefixes.add(ListObjectsResult.CommonPrefix.builder().prefix(commonPrefix).build());
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

    private String escapeRegex(String str) {
        return str.replaceAll("([\\\\\\^\\$\\.\\|\\?\\*\\+\\(\\)\\[\\]\\{\\}])", "\\\\$1");
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
