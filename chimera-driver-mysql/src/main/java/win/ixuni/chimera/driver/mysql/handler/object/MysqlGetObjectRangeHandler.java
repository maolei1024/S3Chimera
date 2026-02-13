package win.ixuni.chimera.driver.mysql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Blob;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 获取对象范围处理器 (Range 请求)
 */
@Slf4j
public class MysqlGetObjectRangeHandler extends AbstractMysqlHandler<GetObjectRangeOperation, S3ObjectData> {

        @Override
        protected Mono<S3ObjectData> doHandle(GetObjectRangeOperation operation, MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();
                long rangeStart = operation.getRangeStart();
                Long rangeEnd = operation.getRangeEnd();
                long handleStartTime = System.currentTimeMillis();

                log.debug("[GET_RANGE] handle() called: bucket={}, key={}, range={}-{}",
                                bucketName, key, rangeStart, rangeEnd);

                // Use context.execute() to get metadata
                return ctx.execute(new HeadObjectOperation(bucketName, key))
                                .doOnSuccess(obj -> log.debug("[GET_RANGE] headObject completed: size={}, elapsed={}ms",
                                                obj.getSize(), System.currentTimeMillis() - handleStartTime))
                                .flatMap(obj -> {
                                        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter()
                                                        .getShardInfo(bucketName, key);
                                        ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

                                        long start = rangeStart;
                                        long end = rangeEnd != null ? rangeEnd : obj.getSize() - 1;

                                        log.debug("[GET_RANGE] Creating content Flux: bucket={}, key={}, range={}-{}, table={}",
                                                        bucketName, key, start, end, shardInfo.getChunkTable());

                                        Flux<ByteBuffer> content = readChunksWithRange(ctx, dataPool,
                                                        shardInfo.getChunkTable(),
                                                        bucketName, key, start, end)
                                                        .doOnSubscribe(s -> log.info(
                                                                        "[GET_RANGE] Content Flux SUBSCRIBED: bucket={}, key={}",
                                                                        bucketName, key))
                                                        .doOnNext(buf -> log.trace(
                                                                        "[GET_RANGE] Emitting buffer: size={} bytes",
                                                                        buf.remaining()))
                                                        .doOnComplete(() -> log.info(
                                                                        "[GET_RANGE] Content Flux COMPLETED: bucket={}, key={}",
                                                                        bucketName, key))
                                                        .doOnCancel(() -> log.warn(
                                                                        "[GET_RANGE] Content Flux CANCELLED: bucket={}, key={}",
                                                                        bucketName, key))
                                                        .doOnError(e -> log.error(
                                                                        "[GET_RANGE] Content Flux ERROR: bucket={}, key={}, error={}",
                                                                        bucketName, key, e.getMessage()));

                                        // 更新元数据的大小为范围大小
                                        S3Object rangeMetadata = S3Object.builder()
                                                        .bucketName(obj.getBucketName())
                                                        .key(obj.getKey())
                                                        .size(end - start + 1)
                                                        .etag(obj.getEtag())
                                                        .lastModified(obj.getLastModified())
                                                        .contentType(obj.getContentType())
                                                        .userMetadata(obj.getUserMetadata())
                                                        .build();

                                        log.debug("[GET_RANGE] Returning S3ObjectData: bucket={}, key={}, rangeSize={}",
                                                        bucketName, key, end - start + 1);

                                        return Mono.just(S3ObjectData.builder()
                                                        .metadata(rangeMetadata)
                                                        .content(content)
                                                        .build());
                                });
        }

        private Flux<ByteBuffer> readChunksWithRange(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
                        String bucketName, String key, long rangeStart, long rangeEnd) {
                int readConcurrency = ctx.getMysqlConfig().getReadConcurrency();
                long startTime = System.currentTimeMillis();

                log.debug("[READ_CHUNKS_RANGE] Starting for {}/{}, table={}, range={}-{}",
                                bucketName, key, tableName, rangeStart, rangeEnd);

                // Query chunk metadata (sorted by part_number, chunk_index for backward compatibility)
                String metaSql = "SELECT part_number, chunk_index, LENGTH(chunk_data) as chunk_size FROM " + tableName +
                                " WHERE bucket_name = ? AND object_key = ? AND upload_id IS NULL ORDER BY part_number, chunk_index";

                // Debug: print actual parameter values and byte info
                log.info("[READ_CHUNKS_RANGE] SQL params - bucket_name='{}' (len={}), object_key='{}' (len={}, bytes={})",
                                bucketName, bucketName.length(), key, key.length(),
                                key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);

                // Use ctx.executeQueryMapped to ensure Row is consumed within the connection
                return ctx.executeQueryMapped(pool, metaSql,
                                row -> {
                                        int partNumber = row.get("part_number", Integer.class);
                                        int chunkIndex = row.get("chunk_index", Integer.class);
                                        int chunkSize = row.get("chunk_size", Integer.class);
                                        log.debug("[READ_CHUNKS_RANGE] Got row: part_number={}, chunk_index={}, chunk_size={}",
                                                        partNumber, chunkIndex, chunkSize);
                                        return new int[] { partNumber, chunkIndex, chunkSize };
                                }, bucketName, key)
                                .doOnSubscribe(s -> log.info("[READ_CHUNKS_RANGE] Query subscribed"))
                                .collectList()
                                .doOnSuccess(list -> log.info(
                                                "[READ_CHUNKS_RANGE] collectList completed, size={}, elapsed={}ms",
                                                list.size(), System.currentTimeMillis() - startTime))
                                .flatMapMany(chunkSizes -> {
                                        if (chunkSizes.isEmpty()) {
                                                log.warn("[READ_CHUNKS_RANGE] No chunks found!");
                                                return Flux.empty();
                                        }

                                        // Compute offset range for each chunk
                                        List<ChunkMeta> metas = new ArrayList<>();
                                        long offset = 0;
                                        for (int[] cs : chunkSizes) {
                                                // cs = [partNumber, chunkIndex, chunkSize]
                                                metas.add(new ChunkMeta(cs[0], cs[1], cs[2], offset,
                                                                offset + cs[2] - 1));
                                                offset += cs[2];
                                        }

                                        // 找出相关的 chunks
                                        List<ChunkMeta> relevant = metas.stream()
                                                        .filter(cm -> cm.endOffset >= rangeStart
                                                                        && cm.startOffset <= rangeEnd)
                                                        .toList();

                                        if (relevant.isEmpty()) {
                                                return Flux.empty();
                                        }

                                        return Flux.fromIterable(relevant)
                                                        .flatMapSequential(cm -> {
                                                                int sliceStart = 0;
                                                                int sliceEnd = cm.size;

                                                                if (cm.startOffset < rangeStart) {
                                                                        sliceStart = (int) (rangeStart
                                                                                        - cm.startOffset);
                                                                }
                                                                if (cm.endOffset > rangeEnd) {
                                                                        sliceEnd = (int) (rangeEnd - cm.startOffset
                                                                                        + 1);
                                                                }

                                                                return readChunkWithSlice(ctx, pool, tableName,
                                                                                bucketName, key,
                                                                                cm.partNumber, cm.chunkIndex,
                                                                                sliceStart, sliceEnd);
                                                        }, readConcurrency);
                                });
        }

        private Mono<ByteBuffer> readChunkWithSlice(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
                        String bucketName, String key, int partNumber, int chunkIndex,
                        int sliceStart, int sliceEnd) {
                // Use (part_number, chunk_index) to locate chunk, compatible with old and new data
                String sql = "SELECT chunk_data FROM " + tableName +
                                " WHERE bucket_name = ? AND object_key = ? AND part_number = ? AND chunk_index = ? AND upload_id IS NULL";

                final long chunkStartTime = System.currentTimeMillis();
                log.debug("[CHUNK_SLICE] Starting: chunk={}, slice=[{}-{}]", chunkIndex, sliceStart, sliceEnd);

                return Mono.usingWhen(
                                Mono.from(pool.create())
                                                .timeout(Duration.ofSeconds(30))
                                                .doOnSubscribe(s -> log.debug(
                                                                "[CHUNK_SLICE] chunk {}: acquiring connection",
                                                                chunkIndex))
                                                .doOnSuccess(c -> log.debug(
                                                                "[CHUNK_SLICE] chunk {}: connection acquired in {}ms",
                                                                chunkIndex,
                                                                System.currentTimeMillis() - chunkStartTime))
                                                .doOnError(e -> log.error(
                                                                "[CHUNK_SLICE] chunk {}: FAILED to acquire connection: {}",
                                                                chunkIndex, e.getMessage())),
                                connection -> {
                                        log.debug("[CHUNK_SLICE] part={}, chunk={}: executing SQL", partNumber,
                                                        chunkIndex);
                                        return Mono.from(connection.createStatement(sql)
                                                        .bind(0, bucketName)
                                                        .bind(1, key)
                                                        .bind(2, partNumber)
                                                        .bind(3, chunkIndex)
                                                        .execute())
                                                        .timeout(Duration.ofSeconds(60))
                                                        .doOnSuccess(r -> log.debug(
                                                                        "[CHUNK_SLICE] chunk {}: SQL executed in {}ms",
                                                                        chunkIndex,
                                                                        System.currentTimeMillis() - chunkStartTime))
                                                        .flatMapMany(result -> Flux.from(result.map((row, meta) -> {
                                                                log.debug("[CHUNK_SLICE] chunk {}: mapping row to blob",
                                                                                chunkIndex);
                                                                Object blobObj = row.get("chunk_data");
                                                                return extractBlobData(blobObj);
                                                        })))
                                                        .flatMap(mono -> mono)
                                                        .next()
                                                        .timeout(Duration.ofSeconds(60))
                                                        .defaultIfEmpty(ByteBuffer.allocate(0))
                                                        .doOnSuccess(bb -> log.debug(
                                                                        "[CHUNK_SLICE] chunk {}: blob extracted, size={} bytes, elapsed={}ms",
                                                                        chunkIndex, bb.remaining(),
                                                                        System.currentTimeMillis() - chunkStartTime))
                                                        .map(bb -> {
                                                                byte[] data = new byte[bb.remaining()];
                                                                bb.get(data);
                                                                if (sliceStart > 0 || sliceEnd < data.length) {
                                                                        int actualSliceEnd = Math.min(sliceEnd,
                                                                                        data.length);
                                                                        int actualSliceStart = Math.min(sliceStart,
                                                                                        actualSliceEnd);
                                                                        byte[] sliced = new byte[actualSliceEnd
                                                                                        - actualSliceStart];
                                                                        if (sliced.length > 0) {
                                                                                System.arraycopy(data, actualSliceStart,
                                                                                                sliced, 0,
                                                                                                sliced.length);
                                                                        }
                                                                        log.debug("[CHUNK_SLICE] chunk {}: sliced [{}-{}] -> {} bytes",
                                                                                        chunkIndex, actualSliceStart,
                                                                                        actualSliceEnd, sliced.length);
                                                                        return ByteBuffer.wrap(sliced);
                                                                }
                                                                return ByteBuffer.wrap(data);
                                                        });
                                },
                                connection -> Mono.from(connection.close())
                                                .doOnSuccess(v -> log.debug("[CHUNK_SLICE] chunk {}: connection closed",
                                                                chunkIndex)));
        }

        private Mono<ByteBuffer> extractBlobData(Object blobObj) {
                if (blobObj == null) {
                        log.debug("[BLOB_EXTRACT] blobObj is null, returning empty buffer");
                        return Mono.just(ByteBuffer.allocate(0));
                }
                if (blobObj instanceof byte[] bytes) {
                        log.debug("[BLOB_EXTRACT] blobObj is byte[], size={}", bytes.length);
                        return Mono.just(ByteBuffer.wrap(bytes));
                }
                if (blobObj instanceof ByteBuffer bb) {
                        log.debug("[BLOB_EXTRACT] blobObj is ByteBuffer, remaining={}", bb.remaining());
                        return Mono.just(bb);
                }
                if (blobObj instanceof Blob blob) {
                        log.debug("[BLOB_EXTRACT] blobObj is Blob, starting stream extraction");
                        long blobStartTime = System.currentTimeMillis();
                        return Flux.from(blob.stream())
                                        .doOnSubscribe(s -> log.debug("[BLOB_EXTRACT] Blob stream subscribed"))
                                        .doOnNext(buf -> log.trace("[BLOB_EXTRACT] Blob stream emitted buffer, size={}",
                                                        buf.remaining()))
                                        .collect(() -> new ByteArrayOutputStream(1024 * 1024),
                                                        (baos, buf) -> {
                                                                byte[] temp = new byte[buf.remaining()];
                                                                buf.get(temp);
                                                                baos.write(temp, 0, temp.length);
                                                        })
                                        .timeout(Duration.ofSeconds(120))
                                        .doOnSuccess(baos -> log.debug(
                                                        "[BLOB_EXTRACT] Blob stream collected, size={} bytes, elapsed={}ms",
                                                        baos.size(), System.currentTimeMillis() - blobStartTime))
                                        .doOnError(e -> log.error("[BLOB_EXTRACT] Blob stream FAILED after {}ms: {}",
                                                        System.currentTimeMillis() - blobStartTime, e.getMessage()))
                                        .map(baos -> ByteBuffer.wrap(baos.toByteArray()))
                                        .doFinally(sig -> {
                                                log.debug("[BLOB_EXTRACT] Blob stream finalized with signal: {}", sig);
                                                Mono.from(blob.discard()).subscribe();
                                        });
                }
                log.warn("[BLOB_EXTRACT] Unknown blob type: {}", blobObj.getClass().getName());
                return Mono.just(ByteBuffer.wrap(blobObj.toString().getBytes()));
        }

        private record ChunkMeta(int partNumber, int chunkIndex, int size, long startOffset, long endOffset) {
        }

        @Override
        public Class<GetObjectRangeOperation> getOperationType() {
                return GetObjectRangeOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.RANGE_READ);
        }
}
