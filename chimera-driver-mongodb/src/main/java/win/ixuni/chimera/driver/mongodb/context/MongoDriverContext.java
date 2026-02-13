package win.ixuni.chimera.driver.mongodb.context;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;
import win.ixuni.chimera.driver.mongodb.MongoDriverFactory;
import win.ixuni.chimera.driver.mongodb.config.MongoDriverConfig;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;

/**
 * MongoDB 驱动上下文
 * 
 * Provides shared dependencies and infrastructure needed by handlers to execute operations.
 */
@Slf4j
@Getter
@Builder
public class MongoDriverContext implements DriverContext {

    // 集合名称常量
    public static final String COLLECTION_BUCKETS = "buckets";
    public static final String COLLECTION_OBJECTS = "objects";
    public static final String COLLECTION_CHUNKS = "chunks";
    public static final String COLLECTION_MULTIPART_UPLOADS = "multipart_uploads";
    public static final String COLLECTION_MULTIPART_PARTS = "multipart_parts";

    /**
     * Driver configuration
     */
    private final DriverConfig config;

    /**
     * MongoDB 特定配置
     */
    private final MongoDriverConfig mongoConfig;

    /**
     * 元数据库客户端
     */
    private final MongoClient metaClient;

    /**
     * 从数据库客户端列表
     */
    private final List<MongoClient> dataClients;

    /**
     * Operation handler registry (injected at runtime)
     */
    @Setter
    private OperationHandlerRegistry handlerRegistry;

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public String getDriverType() {
        return MongoDriverFactory.DRIVER_TYPE;
    }

    /**
     * 获取元数据库
     */
    public MongoDatabase getMetaDatabase() {
        return metaClient.getDatabase(mongoConfig.getMetaDatabase());
    }

    /**
     * Get data database (by shard routing)
     * 
     * @param bucketName S3Bucket 名称
     * @param objectKey  对象 Key
     * @return the corresponding database
     */
    public MongoDatabase getDataDatabase(String bucketName, String objectKey) {
        if (dataClients.isEmpty()) {
            return getMetaDatabase(); // 单库模式
        }
        int dbIndex = calculateDbIndex(bucketName, objectKey);
        MongoDriverConfig.DataSourceConfig dsConfig = mongoConfig.getDataSourceConfig(dbIndex);
        return dataClients.get(dbIndex).getDatabase(dsConfig.getDatabase());
    }

    /**
     * Get the default data database (for cleanup and other non-routed scenarios)
     */
    public MongoDatabase getDefaultDataDatabase() {
        if (dataClients.isEmpty()) {
            return getMetaDatabase(); // 单库模式
        }
        MongoDriverConfig.DataSourceConfig dsConfig = mongoConfig.getDataSourceConfig(0);
        return dataClients.get(0).getDatabase(dsConfig.getDatabase());
    }

    /**
     * Compute database index (based on CRC32 hash)
     */
    private int calculateDbIndex(String bucketName, String objectKey) {
        if (mongoConfig.getDataDbCount() <= 1) {
            return 0;
        }
        String key = bucketName + "/" + objectKey;
        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes(StandardCharsets.UTF_8));
        return (int) (crc32.getValue() % mongoConfig.getDataDbCount());
    }

    /**
     * 获取 S3Bucket 集合
     */
    public MongoCollection<Document> getBucketCollection() {
        return getMetaDatabase().getCollection(COLLECTION_BUCKETS);
    }

    /**
     * 获取对象元数据集合
     */
    public MongoCollection<Document> getObjectCollection() {
        return getMetaDatabase().getCollection(COLLECTION_OBJECTS);
    }

    /**
     * Get data chunk collection (by shard routing)
     */
    public MongoCollection<Document> getChunkCollection(String bucketName, String objectKey) {
        return getDataDatabase(bucketName, objectKey).getCollection(COLLECTION_CHUNKS);
    }

    /**
     * 获取分片上传会话集合
     */
    public MongoCollection<Document> getMultipartUploadCollection() {
        return getMetaDatabase().getCollection(COLLECTION_MULTIPART_UPLOADS);
    }

    /**
     * 获取分片上传 Part 集合
     */
    public MongoCollection<Document> getMultipartPartCollection() {
        return getMetaDatabase().getCollection(COLLECTION_MULTIPART_PARTS);
    }

    /**
     * Date 转 Instant
     */
    public Instant toInstant(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant();
    }

    /**
     * Instant 转 Date
     */
    public Date toDate(Instant instant) {
        if (instant == null) {
            return null;
        }
        return Date.from(instant);
    }
}
