package win.ixuni.chimera.driver.mongodb.handler.object;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * MongoDB GetObjectRange 处理器（范围读取）
 * <p>
 * 支持普通上传和 Multipart 上传后的数据，通过先查询 chunk 元数据
 * Computes cumulative offsets to precisely locate byte ranges, avoiding fixed chunkSize calculation.
 */
@Slf4j
public class MongoGetObjectRangeHandler extends AbstractMongoHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    protected Mono<S3ObjectData> doHandle(GetObjectRangeOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long start = operation.getRangeStart();
        Long end = operation.getRangeEnd();

        // 1. 先获取 object 元数据（包含当前版本号）
        return Mono.from(context.getObjectCollection()
                .find(Filters.and(
                        Filters.eq("bucketName", bucketName),
                        Filters.eq("objectKey", key)))
                .first())
                .switchIfEmpty(Mono.error(new ObjectNotFoundException(bucketName, key)))
                .flatMap(doc -> {
                    long objectSize = doc.getLong("size");
                    String uploadVersion = doc.getString("uploadVersion");

                    long actualEnd = end != null ? Math.min(end, objectSize - 1) : objectSize - 1;
                    long rangeLength = actualEnd - start + 1;

                    // 版本过滤条件
                    org.bson.conversions.Bson versionFilter = uploadVersion != null
                            ? Filters.eq("uploadId", uploadVersion)
                            : Filters.eq("uploadId", null);

                    // 2. First query all chunk metadata (without chunkData), compute cumulative offsets
                    return Flux.from(context.getChunkCollection(bucketName, key)
                            .find(Filters.and(
                                    Filters.eq("bucketName", bucketName),
                                    Filters.eq("objectKey", key),
                                    versionFilter))
                            .projection(Projections.include("partNumber", "chunkIndex", "chunkSize"))
                            .sort(Sorts.ascending("partNumber", "chunkIndex")))
                            .collectList()
                            .flatMap(chunkMetas -> {
                                if (chunkMetas.isEmpty()) {
                                    return Mono.just(S3ObjectData.builder()
                                            .metadata(buildRangeMetadata(doc, bucketName, key, 0))
                                            .content(Flux.empty())
                                            .build());
                                }

                                // Compute cumulative offset range for each chunk to find relevant chunks
                                List<ChunkRange> relevantChunks = new ArrayList<>();
                                long offset = 0;
                                for (Document chunkMeta : chunkMetas) {
                                    int chunkSize = chunkMeta.getInteger("chunkSize");
                                    long chunkStart = offset;
                                    long chunkEnd = offset + chunkSize - 1;

                                    if (chunkEnd >= start && chunkStart <= actualEnd) {
                                        relevantChunks.add(new ChunkRange(
                                                chunkMeta.getInteger("partNumber"),
                                                chunkMeta.getInteger("chunkIndex"),
                                                chunkSize, chunkStart, chunkEnd));
                                    }
                                    offset += chunkSize;
                                }

                                if (relevantChunks.isEmpty()) {
                                    return Mono.just(S3ObjectData.builder()
                                            .metadata(buildRangeMetadata(doc, bucketName, key, 0))
                                            .content(Flux.empty())
                                            .build());
                                }

                                final long finalStart = start;
                                final long finalEnd = actualEnd;

                                // 3. 逐个读取相关 chunk 的数据并裁剪
                                Flux<ByteBuffer> content = Flux.fromIterable(relevantChunks)
                                        .concatMap(chunk -> Mono.from(context.getChunkCollection(bucketName, key)
                                                .find(Filters.and(
                                                        Filters.eq("bucketName", bucketName),
                                                        Filters.eq("objectKey", key),
                                                        versionFilter,
                                                        Filters.eq("partNumber", chunk.partNumber),
                                                        Filters.eq("chunkIndex", chunk.chunkIndex)))
                                                .first())
                                                .map(chunkDoc -> {
                                                    Binary binary = chunkDoc.get("chunkData", Binary.class);
                                                    byte[] data = binary.getData();

                                                    int sliceStart = 0;
                                                    int sliceEnd = data.length;

                                                    if (chunk.startOffset < finalStart) {
                                                        sliceStart = (int) (finalStart - chunk.startOffset);
                                                    }
                                                    if (chunk.endOffset > finalEnd) {
                                                        sliceEnd = (int) (finalEnd - chunk.startOffset + 1);
                                                    }

                                                    byte[] result = new byte[sliceEnd - sliceStart];
                                                    System.arraycopy(data, sliceStart, result, 0, result.length);
                                                    return ByteBuffer.wrap(result);
                                                }));

                                return Mono.just(S3ObjectData.builder()
                                        .metadata(buildRangeMetadata(doc, bucketName, key, rangeLength))
                                        .content(content)
                                        .build());
                            });
                })
                .doOnNext(obj -> log.debug("GetObjectRange: bucket={}, key={}, range={}-{}, size={}",
                        bucketName, key, start, end, obj.getMetadata().getSize()));
    }

    private S3Object buildRangeMetadata(Document doc, String bucketName, String key, long rangeLength) {
        return S3Object.builder()
                .bucketName(bucketName)
                .key(key)
                .size(rangeLength)
                .etag(doc.getString("etag"))
                .contentType(doc.getString("contentType"))
                .lastModified(doc.getDate("lastModified").toInstant())
                .storageClass(doc.getString("storageClass"))
                .build();
    }

    private record ChunkRange(int partNumber, int chunkIndex, int size, long startOffset, long endOffset) {
    }

    @Override
    public Class<GetObjectRangeOperation> getOperationType() {
        return GetObjectRangeOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ, Capability.RANGE_READ);
    }
}
