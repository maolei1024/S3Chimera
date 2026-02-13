package win.ixuni.chimera.driver.mongodb.handler.multipart;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.multipart.ListMultipartUploadsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB 列出分片上传会话处理器
 */
@Slf4j
public class MongoListMultipartUploadsHandler
        extends AbstractMongoHandler<ListMultipartUploadsOperation, ListMultipartUploadsResult> {

    @Override
    protected Mono<ListMultipartUploadsResult> doHandle(ListMultipartUploadsOperation operation,
            MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String prefix = operation.getPrefix();
        int maxUploads = 1000;

        List<org.bson.conversions.Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("bucketName", bucketName));
        if (prefix != null && !prefix.isEmpty()) {
            filters.add(Filters.regex("objectKey", "^" + escapeRegex(prefix)));
        }

        org.bson.conversions.Bson filter = Filters.and(filters);

        return Flux.from(context.getMultipartUploadCollection()
                .find(filter)
                .sort(Sorts.ascending("objectKey", "uploadId"))
                .limit(maxUploads + 1))
                .collectList()
                .map(docs -> {
                    boolean isTruncated = docs.size() > maxUploads;
                    String nextKeyMarker = null;
                    String nextUploadIdMarker = null;

                    List<MultipartUpload> uploads = new ArrayList<>();
                    int count = 0;
                    for (Document doc : docs) {
                        if (count >= maxUploads)
                            break;
                        String uploadId = doc.getString("uploadId");
                        String objectKey = doc.getString("objectKey");
                        uploads.add(MultipartUpload.builder()
                                .uploadId(uploadId)
                                .bucketName(bucketName)
                                .key(objectKey)
                                .initiated(context.toInstant(doc.getDate("initiatedAt")))
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

    private String escapeRegex(String str) {
        return str.replaceAll("([\\\\\\^\\$\\.\\|\\?\\*\\+\\(\\)\\[\\]\\{\\}])", "\\\\$1");
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
