package win.ixuni.chimera.driver.mongodb.handler.multipart;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListPartsRequest;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.multipart.ListPartsOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB 列出分片处理器
 */
@Slf4j
public class MongoListPartsHandler extends AbstractMongoHandler<ListPartsOperation, ListPartsResult> {

    @Override
    protected Mono<ListPartsResult> doHandle(ListPartsOperation operation, MongoDriverContext context) {
        ListPartsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();
        String key = request.getKey();
        String uploadId = request.getUploadId();
        int maxParts = request.getMaxParts() != null && request.getMaxParts() > 0 ? request.getMaxParts() : 1000;
        int partNumberMarker = request.getPartNumberMarker() != null ? request.getPartNumberMarker() : 0;

        // 根据 S3 标准，先检查 uploadId 是否存在
        return Mono.from(context.getMultipartUploadCollection()
                .find(Filters.eq("uploadId", uploadId))
                .first())
                .switchIfEmpty(Mono.error(new win.ixuni.chimera.core.exception.NoSuchUploadException(uploadId)))
                .flatMapMany(upload -> Flux.from(context.getMultipartPartCollection()
                        .find(Filters.and(
                                Filters.eq("uploadId", uploadId),
                                Filters.gt("partNumber", partNumberMarker)))
                        .sort(Sorts.ascending("partNumber"))
                        .limit(maxParts + 1)))
                .collectList()
                .map(docs -> {
                    boolean isTruncated = docs.size() > maxParts;
                    int nextPartNumberMarker = 0;

                    var parts = docs.stream()
                            .limit(maxParts)
                            .map(doc -> UploadPart.builder()
                                    .partNumber(doc.getInteger("partNumber"))
                                    .size(doc.getLong("size"))
                                    .etag(doc.getString("etag"))
                                    .lastModified(context.toInstant(doc.getDate("uploadedAt")))
                                    .build())
                            .toList();

                    if (!parts.isEmpty()) {
                        nextPartNumberMarker = parts.get(parts.size() - 1).getPartNumber();
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
