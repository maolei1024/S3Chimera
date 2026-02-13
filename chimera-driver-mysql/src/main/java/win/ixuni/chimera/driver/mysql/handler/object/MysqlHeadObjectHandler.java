package win.ixuni.chimera.driver.mysql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.time.LocalDateTime;
import java.util.Map;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 获取对象元数据处理器
 */
@Slf4j
public class MysqlHeadObjectHandler extends AbstractMysqlHandler<HeadObjectOperation, S3Object> {

        @Override
        protected Mono<S3Object> doHandle(HeadObjectOperation operation, MysqlDriverContext ctx) {
                String bucketName = operation.getBucketName();
                String key = operation.getKey();

                ShardingRouter.ShardInfo shardInfo = ctx.getShardingRouter().getShardInfo(bucketName, key);

                String sql = "SELECT bucket_name, object_key, size, etag, content_type, last_modified " +
                                "FROM " + shardInfo.getObjectTable() + " WHERE bucket_name = ? AND object_key = ?";

                // Use Mono.usingWhen to ensure Row is fully consumed before connection closes
                // 这避免了 R2DBC MySQL 驱动在并发场景下的 refCnt:0 问题
                return Mono.usingWhen(
                                Mono.from(ctx.getMetaConnectionPool().create()),
                                connection -> {
                                        var stmt = connection.createStatement(sql);
                                        stmt.bind(0, bucketName);
                                        stmt.bind(1, key);
                                        return Mono.from(stmt.execute())
                                                        .flatMapMany(result -> Flux.from(result.map((row, meta) -> {
                                                                // Extract all data immediately inside result.map callback, ensuring Row's ByteBuf is still valid
                                                                return S3Object.builder()
                                                                                .bucketName(row.get("bucket_name",
                                                                                                String.class))
                                                                                .key(row.get("object_key",
                                                                                                String.class))
                                                                                .size(row.get("size", Long.class))
                                                                                .etag(row.get("etag", String.class))
                                                                                .contentType(row.get("content_type",
                                                                                                String.class))
                                                                                .lastModified(ctx.toInstant(
                                                                                                row.get("last_modified",
                                                                                                                LocalDateTime.class)))
                                                                                .storageClass("STANDARD")
                                                                                .build();
                                                        })))
                                                        .next();
                                },
                                connection -> Mono.from(connection.close()))
                                .switchIfEmpty(Mono.error(new ObjectNotFoundException(bucketName, key)))
                                .flatMap(obj -> loadUserMetadata(ctx, bucketName, key, shardInfo)
                                                .map(metadata -> S3Object.builder()
                                                                .bucketName(obj.getBucketName())
                                                                .key(obj.getKey())
                                                                .size(obj.getSize())
                                                                .etag(obj.getEtag())
                                                                .contentType(obj.getContentType())
                                                                .lastModified(obj.getLastModified())
                                                                .storageClass(obj.getStorageClass())
                                                                .userMetadata(metadata)
                                                                .build())
                                                .defaultIfEmpty(obj));
        }

        private Mono<Map<String, String>> loadUserMetadata(MysqlDriverContext ctx, String bucketName,
                        String key, ShardingRouter.ShardInfo shardInfo) {
                String sql = "SELECT meta_key, meta_value FROM " + shardInfo.getObjectMetadataTable() +
                                " WHERE bucket_name = ? AND object_key = ?";

                // Use Mono.usingWhen to ensure Row is fully consumed before connection closes
                return Mono.usingWhen(
                                Mono.from(ctx.getMetaConnectionPool().create()),
                                connection -> {
                                        var stmt = connection.createStatement(sql);
                                        stmt.bind(0, bucketName);
                                        stmt.bind(1, key);
                                        return Flux.from(stmt.execute())
                                                        .flatMap(result -> Flux.from(result.map((row, meta) -> {
                                                                // 在 result.map 回调内立即提取数据
                                                                String metaKey = row.get("meta_key", String.class);
                                                                String metaValue = row.get("meta_value", String.class);
                                                                return new String[] { metaKey, metaValue };
                                                        })))
                                                        .collectMap(arr -> arr[0], arr -> arr[1]);
                                },
                                connection -> Mono.from(connection.close()))
                                .filter(map -> !map.isEmpty());
        }

        @Override
        public Class<HeadObjectOperation> getOperationType() {
                return HeadObjectOperation.class;
        }

        @Override
        public Set<Capability> getProvidedCapabilities() {
                return EnumSet.of(Capability.READ);
        }
}
