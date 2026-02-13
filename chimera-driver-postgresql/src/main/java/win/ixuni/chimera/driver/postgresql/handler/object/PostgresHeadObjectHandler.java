package win.ixuni.chimera.driver.postgresql.handler.object;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.AbstractPostgresHandler;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL HeadObject 处理器
 */
@Slf4j
public class PostgresHeadObjectHandler extends AbstractPostgresHandler<HeadObjectOperation, S3Object> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected Mono<S3Object> doHandle(HeadObjectOperation operation, PostgresDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        String sql = "SELECT size, etag, content_type, last_modified, storage_class, user_metadata " +
                "FROM t_object WHERE bucket_name = $1 AND object_key = $2";

        return context.executeQuery(context.getMetaConnectionPool(), sql, bucketName, key)
                .next()
                .map(row -> {
                    Map<String, String> userMetadata = new HashMap<>();
                    String jsonMetadata = row.get("user_metadata", String.class);

                    if (jsonMetadata != null && !jsonMetadata.isEmpty()) {
                        try {
                            Map<String, Object> map = objectMapper.readValue(jsonMetadata,
                                    new TypeReference<Map<String, Object>>() {
                                    });
                            if (map != null) {
                                map.forEach((k, v) -> userMetadata.put(k, v != null ? v.toString() : null));
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse user_metadata for {}/{}: {}", bucketName, key, e.getMessage());
                        }
                    }

                    return S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size(row.get("size", Long.class))
                            .contentType(row.get("content_type", String.class))
                            .etag(row.get("etag", String.class))
                            .lastModified(context.toInstant(row.get("last_modified", LocalDateTime.class)))
                            .storageClass(row.get("storage_class", String.class))
                            .userMetadata(userMetadata)
                            .build();
                })
                .doOnNext(meta -> log.debug("HeadObject: bucket={}, key={}, size={}",
                        bucketName, key, meta.getSize()));
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
