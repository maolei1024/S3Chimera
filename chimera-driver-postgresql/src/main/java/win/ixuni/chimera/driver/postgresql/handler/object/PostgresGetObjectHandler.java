package win.ixuni.chimera.driver.postgresql.handler.object;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL GetObject 处理器
 */
@Slf4j
public class PostgresGetObjectHandler extends AbstractPostgresHandler<GetObjectOperation, S3ObjectData> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected Mono<S3ObjectData> doHandle(GetObjectOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 1. 先获取 object 元数据（包含当前版本号）
        return getObjectMetadata(context, bucketName, key)
                .flatMap(metadataWithVersion -> {
                    S3Object metadata = metadataWithVersion.metadata;
                    String uploadVersion = metadataWithVersion.uploadVersion;

                    ShardingRouter.ShardInfo shardInfo = context.getShardingRouter().getShardInfo(bucketName, key);
                    ConnectionPool dataPool = context.getDataConnectionPool(bucketName, key);

                    // 2. 根据版本号读取 chunks
                    Flux<ByteBuffer> content = readChunks(dataPool, shardInfo.getChunkTable(),
                            bucketName, key, uploadVersion);

                    return Mono.just(S3ObjectData.builder()
                            .metadata(metadata)
                            .content(content)
                            .build());
                })
                .doOnNext(obj -> log.debug("GetObject: bucket={}, key={}", bucketName, key));
    }

    private Mono<MetadataWithVersion> getObjectMetadata(PostgresDriverContext context, String bucketName, String key) {
        String sql = "SELECT size, etag, content_type, last_modified, storage_class, user_metadata, upload_version " +
                "FROM t_object WHERE bucket_name = $1 AND object_key = $2";

        return context.executeQuery(context.getMetaConnectionPool(), sql, bucketName, key)
                .next()
                .switchIfEmpty(Mono.error(new ObjectNotFoundException(bucketName, key)))
                .map(row -> {
                    Map<String, String> userMetadata = new HashMap<>();
                    String metadataJson = row.get("user_metadata", String.class);
                    if (metadataJson != null && !metadataJson.isEmpty()) {
                        parseJsonMetadata(metadataJson, userMetadata);
                    }

                    S3Object metadata = S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size(row.get("size", Long.class))
                            .etag(row.get("etag", String.class))
                            .contentType(row.get("content_type", String.class))
                            .lastModified(context.toInstant(row.get("last_modified", java.time.LocalDateTime.class)))
                            .storageClass(row.get("storage_class", String.class))
                            .userMetadata(userMetadata)
                            .build();

                    String uploadVersion = row.get("upload_version", String.class);
                    return new MetadataWithVersion(metadata, uploadVersion);
                });
    }

    private void parseJsonMetadata(String json, Map<String, String> result) {
        try {
            Map<String, Object> map = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
            if (map != null) {
                map.forEach((k, v) -> result.put(k, v != null ? v.toString() : null));
            }
        } catch (Exception e) {
            log.warn("Failed to parse user_metadata: {}", e.getMessage());
        }
    }

    /**
     * 流式读取 chunks（兼容新旧数据格式）
     * 
     * 排序策略：ORDER BY part_number, chunk_index
     * - 旧数据：part_number = 0，退化为 ORDER BY chunk_index
     * - 新数据（multipart）：按 part 分组，组内按 chunk_index 排序
     */
    private Flux<ByteBuffer> readChunks(ConnectionPool pool, String tableName, String bucketName,
            String key, String uploadVersion) {
        // Stream-read all chunks, sorted by (part_number, chunk_index)
        String sql = uploadVersion != null
                ? "SELECT chunk_data FROM " + tableName +
                        " WHERE bucket_name = $1 AND object_key = $2 AND upload_id = $3" +
                        " ORDER BY part_number, chunk_index"
                : "SELECT chunk_data FROM " + tableName +
                        " WHERE bucket_name = $1 AND object_key = $2 AND upload_id IS NULL" +
                        " ORDER BY part_number, chunk_index";

        return Flux.usingWhen(
                Mono.from(pool.create())
                        .doOnSubscribe(s -> log.debug("[READ_CHUNKS] Acquiring connection for streaming read")),
                connection -> {
                    var stmt = connection.createStatement(sql)
                            .bind("$1", bucketName)
                            .bind("$2", key);
                    if (uploadVersion != null) {
                        stmt.bind("$3", uploadVersion);
                    }
                    return Flux.from(stmt.execute())
                            .flatMap(result -> Flux.from(result.map((row, meta) -> {
                                byte[] data = row.get("chunk_data", byte[].class);
                                return data != null ? ByteBuffer.wrap(data) : ByteBuffer.allocate(0);
                            })))
                            .doOnComplete(() -> log.debug("[READ_CHUNKS] Completed streaming read"));
                },
                connection -> Mono.from(connection.close())
                        .doOnSuccess(v -> log.debug("[READ_CHUNKS] Connection released")));
    }

    private static class MetadataWithVersion {
        final S3Object metadata;
        final String uploadVersion;

        MetadataWithVersion(S3Object metadata, String uploadVersion) {
            this.metadata = metadata;
            this.uploadVersion = uploadVersion;
        }
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
