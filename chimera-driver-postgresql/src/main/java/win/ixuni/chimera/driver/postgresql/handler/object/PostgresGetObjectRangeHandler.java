package win.ixuni.chimera.driver.postgresql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.Set;

/**
 * PostgreSQL GetObjectRange 处理器（范围读取）
 */
@Slf4j
public class PostgresGetObjectRangeHandler extends AbstractPostgresHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    protected Mono<S3ObjectData> doHandle(GetObjectRangeOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long start = operation.getRangeStart();
        Long end = operation.getRangeEnd();

        // 查询元数据时包含 upload_version
        String metaSql = "SELECT size, etag, content_type, last_modified, storage_class, chunk_size, upload_version " +
                "FROM t_object WHERE bucket_name = $1 AND object_key = $2";

        return context.executeQuery(context.getMetaConnectionPool(), metaSql, bucketName, key)
                .next()
                .flatMap(row -> {
                    long objectSize = row.get("size", Long.class);
                    int chunkSize = row.get("chunk_size", Integer.class);
                    String uploadVersion = row.get("upload_version", String.class);

                    long actualEnd = end != null ? Math.min(end, objectSize - 1) : objectSize - 1;
                    long rangeLength = actualEnd - start + 1;

                    int startChunk = (int) (start / chunkSize);
                    int endChunk = (int) (actualEnd / chunkSize);
                    long offsetInFirstChunk = start % chunkSize;
                    long finalActualEnd = actualEnd;

                    ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);
                    ConnectionPool dataPool = context.getDataConnectionPool(bucketName, key);

                    // Use upload_version to match chunks (consistent with GetObjectHandler)
                    Flux<ByteBuffer> content;
                    if (uploadVersion != null) {
                        String chunkSql = "SELECT chunk_index, chunk_data FROM " + shardInfo.getChunkTable() +
                                " WHERE bucket_name = $1 AND object_key = $2 AND upload_id = $5 " +
                                "AND chunk_index >= $3 AND chunk_index <= $4 ORDER BY chunk_index";
                        content = context.executeQuery(dataPool, chunkSql,
                                bucketName, key, startChunk, endChunk, uploadVersion)
                                .map(chunkRow -> extractChunkData(chunkRow, startChunk, endChunk,
                                        offsetInFirstChunk, finalActualEnd, chunkSize));
                    } else {
                        String chunkSql = "SELECT chunk_index, chunk_data FROM " + shardInfo.getChunkTable() +
                                " WHERE bucket_name = $1 AND object_key = $2 AND upload_id IS NULL " +
                                "AND chunk_index >= $3 AND chunk_index <= $4 ORDER BY chunk_index";
                        content = context.executeQuery(dataPool, chunkSql,
                                bucketName, key, startChunk, endChunk)
                                .map(chunkRow -> extractChunkData(chunkRow, startChunk, endChunk,
                                        offsetInFirstChunk, finalActualEnd, chunkSize));
                    }

                    S3Object rangeMetadata = S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size(rangeLength)
                            .etag(row.get("etag", String.class))
                            .contentType(row.get("content_type", String.class))
                            .lastModified(context.toInstant(row.get("last_modified", LocalDateTime.class)))
                            .storageClass(row.get("storage_class", String.class))
                            .build();

                    return Mono.just(S3ObjectData.builder()
                            .metadata(rangeMetadata)
                            .content(content)
                            .build());
                })
                .doOnNext(obj -> log.debug("GetObjectRange: bucket={}, key={}, range={}-{}, size={}",
                        bucketName, key, start, end, obj.getMetadata().getSize()));
    }

    private ByteBuffer extractChunkData(io.r2dbc.spi.Row chunkRow, int startChunk, int endChunk,
            long offsetInFirstChunk, long finalActualEnd, int chunkSize) {
        int chunkIdx = chunkRow.get("chunk_index", Integer.class);
        byte[] data = chunkRow.get("chunk_data", byte[].class);

        int skipBytes = 0;
        int takeBytes = data.length;

        if (chunkIdx == startChunk) {
            skipBytes = (int) offsetInFirstChunk;
            takeBytes = data.length - skipBytes;
        }
        if (chunkIdx == endChunk) {
            long endOffset = finalActualEnd % chunkSize;
            if (chunkIdx == startChunk) {
                takeBytes = (int) (endOffset - offsetInFirstChunk + 1);
            } else {
                takeBytes = (int) (endOffset + 1);
            }
        }

        byte[] result = new byte[takeBytes];
        System.arraycopy(data, skipBytes, result, 0, takeBytes);
        return ByteBuffer.wrap(result);
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
