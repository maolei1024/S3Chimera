package win.ixuni.chimera.driver.mysql.handler.object;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Blob;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 获取对象处理器（包含数据流）
 */
@Slf4j
public class MysqlGetObjectHandler extends AbstractMysqlHandler<GetObjectOperation, S3ObjectData> {

        @Override
        protected Mono<S3ObjectData> doHandle(GetObjectOperation operation, MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();

                // Use context.execute() to get metadata
                return ctx.execute(new HeadObjectOperation(bucketName, key))
                                .flatMap(obj -> {
                                        ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter()
                                                        .getShardInfo(bucketName, key);
                                        ConnectionPool dataPool = ctx.getDataConnectionPool(bucketName, key);

                                        Flux<ByteBuffer> content = readChunks(ctx, dataPool, shardInfo.getChunkTable(),
                                                        bucketName, key);

                                        return Mono.just(S3ObjectData.builder()
                                                        .metadata(obj)
                                                        .content(content)
                                                        .build());
                                });
        }

        /**
         * 流式读取 chunks（兼容新旧数据格式）
         * 
         * 排序策略：ORDER BY part_number, chunk_index
         * - 旧数据：part_number = 0，退化为 ORDER BY chunk_index
         * - 新数据（multipart）：按 part 分组，组内按 chunk_index 排序
         */
        private Flux<ByteBuffer> readChunks(MysqlDriverContext ctx, ConnectionPool pool, String tableName,
                        String bucketName, String key) {
                long startTime = System.currentTimeMillis();

                log.debug("[READ_CHUNKS] Starting for {}/{}, table={}", bucketName, key, tableName);

                // Stream-read all chunks, sorted by (part_number, chunk_index)
                String sql = "SELECT chunk_data FROM " + tableName +
                                " WHERE bucket_name = ? AND object_key = ? AND upload_id IS NULL " +
                                " ORDER BY part_number, chunk_index";

                return Flux.usingWhen(
                                Mono.from(pool.create())
                                                .doOnSubscribe(s -> log.debug(
                                                                "[READ_CHUNKS] Acquiring connection for streaming read")),
                                connection -> {
                                        var stmt = connection.createStatement(sql)
                                                        .bind(0, bucketName)
                                                        .bind(1, key);
                                        return Flux.from(stmt.execute())
                                                        .flatMap(result -> Flux
                                                                        .from(result.map((row, meta) -> extractBlobData(
                                                                                        row.get("chunk_data")))))
                                                        .flatMap(mono -> mono)
                                                        .doOnComplete(() -> log.debug(
                                                                        "[READ_CHUNKS] Completed streaming read in {}ms",
                                                                        System.currentTimeMillis() - startTime));
                                },
                                connection -> Mono.from(connection.close())
                                                .doOnSuccess(v -> log.debug(
                                                                "[READ_CHUNKS] Connection released")));
        }

        private Mono<ByteBuffer> extractBlobData(Object blobObj) {
                if (blobObj == null) {
                        return Mono.just(ByteBuffer.allocate(0));
                }
                if (blobObj instanceof byte[] bytes) {
                        return Mono.just(ByteBuffer.wrap(bytes));
                }
                if (blobObj instanceof ByteBuffer bb) {
                        return Mono.just(bb);
                }
                if (blobObj instanceof Blob blob) {
                        return Flux.from(blob.stream())
                                        .collect(
                                                        () -> new ByteArrayOutputStream(1024 * 1024),
                                                        (baos, buf) -> {
                                                                byte[] temp = new byte[buf.remaining()];
                                                                buf.get(temp);
                                                                baos.write(temp, 0, temp.length);
                                                        })
                                        .map(baos -> ByteBuffer.wrap(baos.toByteArray()))
                                        .doFinally(sig -> Mono.from(blob.discard()).subscribe());
                }
                log.warn("Unknown blob type: {}", blobObj.getClass().getName());
                return Mono.just(ByteBuffer.wrap(blobObj.toString().getBytes()));
        }

        @Override
        public Class<GetObjectOperation> getOperationType() {
                return GetObjectOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.READ);
        }
}
