package win.ixuni.chimera.driver.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.driver.CleanableDriver;
import win.ixuni.chimera.driver.mongodb.config.MongoDriverConfig;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.bucket.*;
import win.ixuni.chimera.driver.mongodb.handler.multipart.*;
import win.ixuni.chimera.driver.mongodb.handler.object.*;
import win.ixuni.chimera.driver.mongodb.schema.MongoSchemaInitializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * MongoDB storage driver V2
 * 
 * Implements the new command-pattern architecture.
 * Each operation is handled by an independent handler; the driver only initializes and composes them.
 */
@Slf4j
public class MongoStorageDriverV2 extends AbstractStorageDriverV2 implements CleanableDriver {

    private final DriverConfig config;

    @Getter
    private MongoDriverContext driverContext;

    private MongoDriverConfig mongoConfig;
    private MongoClient metaClient;
    private final List<MongoClient> dataClients = new ArrayList<>();

    public MongoStorageDriverV2(DriverConfig config) {
        this.config = config;
    }

    @Override
    public String getDriverType() {
        return MongoDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        return Mono.fromRunnable(this::parseConfig)
                .then(Mono.fromRunnable(this::createClients))
                .then(Mono.fromRunnable(this::buildDriverContext))
                .then(Mono.fromRunnable(this::registerHandlers))
                .then(Mono.defer(this::initializeSchema))
                .doOnSuccess(v -> log.info("MongoDB 驱动 V2 '{}' 初始化完成，共 {} 个处理器",
                        getDriverName(), getHandlerRegistry().size()))
                .doOnError(e -> log.error("MongoDB 驱动 V2 '{}' 初始化失败", getDriverName(), e));
    }

    private void parseConfig() {
        String uri = config.getString("uri", "mongodb://localhost:27017");
        String database = config.getString("database", "chimera_meta");
        String username = config.getString("username", "");
        String password = config.getString("password", "");

        // 解析数据源配置
        List<MongoDriverConfig.DataSourceConfig> dataSources = parseDataSources();

        int chunkSize = parseDataSize(config.getString("chunk-size", "4MB"));
        int poolSize = config.getInt("pool-size", 10);
        int writeConcurrency = config.getInt("write-concurrency", 2);
        int readConcurrency = config.getInt("read-concurrency", 4);
        boolean autoCreateSchema = config.getBoolean("auto-create-schema", true);

        this.mongoConfig = MongoDriverConfig.builder()
                .metaUri(uri)
                .metaDatabase(database)
                .metaUsername(username.isEmpty() ? null : username)
                .metaPassword(password.isEmpty() ? null : password)
                .dataSources(dataSources)
                .chunkSize(chunkSize)
                .poolSize(poolSize)
                .writeConcurrency(writeConcurrency)
                .readConcurrency(readConcurrency)
                .autoCreateSchema(autoCreateSchema)
                .build();

        log.info("MongoDB 驱动 V2 配置解析完成: uri={}, database={}, chunkSize={}KB",
                uri, database, chunkSize / 1024);
    }

    @SuppressWarnings("unchecked")
    private List<MongoDriverConfig.DataSourceConfig> parseDataSources() {
        List<MongoDriverConfig.DataSourceConfig> result = new ArrayList<>();
        Object dataSourcesObj = config.getProperty("data-sources", null);

        if (dataSourcesObj == null) {
            log.info("[PARSE_DS] data-sources is empty - using single-database mode");
            return null;
        }

        // 情况1: 标准 List 格式
        if (dataSourcesObj instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                parseDataSourceItem(item, i, result);
            }
        }
        // Case 2: Spring Boot parses YAML lists as Map format (key is index)
        else if (dataSourcesObj instanceof Map<?, ?> map) {
            log.info("[PARSE_DS] data-sources be Map with {} entries", map.size());
            int index = 0;
            // Map is usually LinkedHashMap with guaranteed order, but may also be unordered
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                Object item = entry.getValue();
                parseDataSourceItem(item, index++, result);
            }
        }

        log.info("[PARSE_DS] 解析到 {} 个数据源", result.size());
        return result.isEmpty() ? null : result;
    }

    @SuppressWarnings("unchecked")
    private void parseDataSourceItem(Object item, int index, List<MongoDriverConfig.DataSourceConfig> result) {
        if (item instanceof Map<?, ?> map) {
            Map<String, Object> dsMap = (Map<String, Object>) map;
            String dsUri = getMapString(dsMap, "uri", "");
            if (!dsUri.isEmpty()) {
                result.add(MongoDriverConfig.DataSourceConfig.builder()
                        .name(getMapString(dsMap, "name", "data-" + index))
                        .uri(dsUri)
                        .database(getMapString(dsMap, "database", "chimera_data_" + index))
                        .username(getMapString(dsMap, "username", null))
                        .password(getMapString(dsMap, "password", null))
                        .build());
            }
        }
    }

    private String getMapString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private int parseDataSize(String sizeStr) {
        if (sizeStr == null || sizeStr.isEmpty())
            return 4 * 1024 * 1024;
        sizeStr = sizeStr.trim().toUpperCase();
        try {
            if (sizeStr.endsWith("MB"))
                return Integer.parseInt(sizeStr.replace("MB", "").trim()) * 1024 * 1024;
            if (sizeStr.endsWith("KB"))
                return Integer.parseInt(sizeStr.replace("KB", "").trim()) * 1024;
            return Integer.parseInt(sizeStr);
        } catch (NumberFormatException e) {
            return 4 * 1024 * 1024;
        }
    }

    private void createClients() {
        // 创建元数据客户端（包含认证信息）
        this.metaClient = createClient(
                mongoConfig.getMetaUri(),
                mongoConfig.getMetaDatabase(),
                mongoConfig.getMetaUsername(),
                mongoConfig.getMetaPassword(),
                "meta");

        int dataDbCount = mongoConfig.getDataDbCount();
        for (int i = 0; i < dataDbCount; i++) {
            MongoDriverConfig.DataSourceConfig dsConfig = mongoConfig.getDataSourceConfig(i);
            if (dsConfig != null) {
                // 创建数据分片客户端（包含认证信息）
                dataClients.add(createClient(
                        dsConfig.getUri(),
                        dsConfig.getDatabase(),
                        dsConfig.getUsername(),
                        dsConfig.getPassword(),
                        dsConfig.getName()));
            }
        }
    }

    private MongoClient createClient(String uri, String database, String username, String password, String name) {
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                .applyConnectionString(connectionString);

        // If username and password are configured but not in URI, add auth credentials manually
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            // If URI has no auth info, add it
            if (connectionString.getCredential() == null) {
                log.info("为 MongoDB 客户端 '{}' 添加认证凭据: user={}, db={}", name, username, database);
                settingsBuilder.credential(com.mongodb.MongoCredential.createCredential(
                        username, database, password.toCharArray()));
            }
        }

        MongoClientSettings settings = settingsBuilder.build();

        log.debug("创建 MongoDB 客户端: {}", name);
        return MongoClients.create(settings);
    }

    private void buildDriverContext() {
        this.driverContext = MongoDriverContext.builder()
                .config(config)
                .mongoConfig(mongoConfig)
                .metaClient(metaClient)
                .dataClients(dataClients)
                .build();
    }

    private void registerHandlers() {
        // S3Bucket 处理器 (4)
        getHandlerRegistry().register(new MongoCreateBucketHandler());
        getHandlerRegistry().register(new MongoDeleteBucketHandler());
        getHandlerRegistry().register(new MongoBucketExistsHandler());
        getHandlerRegistry().register(new MongoListBucketsHandler());

        // Object 处理器 (9)
        getHandlerRegistry().register(new MongoHeadObjectHandler());
        getHandlerRegistry().register(new MongoGetObjectHandler());
        getHandlerRegistry().register(new MongoDeleteObjectHandler());
        getHandlerRegistry().register(new MongoPutObjectHandler());
        getHandlerRegistry().register(new MongoCopyObjectHandler());
        getHandlerRegistry().register(new MongoDeleteObjectsHandler());
        getHandlerRegistry().register(new MongoListObjectsHandler());
        getHandlerRegistry().register(new MongoListObjectsV2Handler());
        getHandlerRegistry().register(new MongoGetObjectRangeHandler());

        // Multipart 处理器 (6)
        getHandlerRegistry().register(new MongoCreateMultipartUploadHandler());
        getHandlerRegistry().register(new MongoUploadPartHandler());
        getHandlerRegistry().register(new MongoCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new MongoAbortMultipartUploadHandler());
        getHandlerRegistry().register(new MongoListPartsHandler());
        getHandlerRegistry().register(new MongoListMultipartUploadsHandler());

        log.info("注册 {} 个操作处理器", getHandlerRegistry().size());

        // 注入 handlerRegistry 到 context
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private Mono<Void> initializeSchema() {
        if (!mongoConfig.isAutoCreateSchema()) {
            return Mono.empty();
        }

        MongoSchemaInitializer initializer = new MongoSchemaInitializer(
                mongoConfig, metaClient, dataClients);
        return initializer.initialize();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("关闭 MongoDB 驱动 V2: {}", getDriverName());

        List<Mono<Void>> closeTasks = new ArrayList<>();
        if (metaClient != null) {
            closeTasks.add(Mono.fromRunnable(() -> metaClient.close()));
        }
        for (MongoClient client : dataClients) {
            closeTasks.add(Mono.fromRunnable(client::close));
        }

        return Mono.when(closeTasks);
    }

    // ==================== CleanableDriver 实现 ====================

    /**
     * Clean up orphan chunks (with uploadId but creation time exceeds threshold)
     */
    @Override
    public Mono<Long> cleanupOrphanChunks(int olderThanHours) {
        log.info("开始清理孤儿 chunks, olderThanHours={}", olderThanHours);

        Date threshold = Date.from(Instant.now().minus(Duration.ofHours(olderThanHours)));

        // If there are multiple databases, iterate all databases for cleanup
        if (!dataClients.isEmpty()) {
            return Flux.range(0, mongoConfig.getDataDbCount())
                    .flatMap(index -> cleanupOrphanChunksInDb(index, threshold))
                    .reduce(0L, Long::sum)
                    .doOnSuccess(count -> {
                        if (count > 0) {
                            log.info("清理了 {} 个孤儿 chunks", count);
                        }
                    });
        } else {
            // 单库模式
            return cleanupOrphanChunksInMetaDb(threshold)
                    .doOnSuccess(count -> {
                        if (count > 0) {
                            log.info("清理了 {} 个孤儿 chunks", count);
                        }
                    });
        }
    }

    private Mono<Long> cleanupOrphanChunksInDb(int dbIndex, Date threshold) {
        MongoDriverConfig.DataSourceConfig dsConfig = mongoConfig.getDataSourceConfig(dbIndex);
        var db = dataClients.get(dbIndex).getDatabase(dsConfig.getDatabase());
        var chunksCollection = db.getCollection(MongoDriverContext.COLLECTION_CHUNKS);

        return Mono.from(chunksCollection.deleteMany(
                Filters.and(
                        Filters.exists("uploadId"),
                        Filters.ne("uploadId", null),
                        Filters.lt("createdAt", threshold))))
                .map(result -> result.getDeletedCount())
                .onErrorResume(e -> {
                    log.warn("清理数据库 {} 中的孤儿 chunks 失败: {}", dbIndex, e.getMessage());
                    return Mono.just(0L);
                });
    }

    private Mono<Long> cleanupOrphanChunksInMetaDb(Date threshold) {
        var chunksCollection = driverContext.getMetaDatabase().getCollection(MongoDriverContext.COLLECTION_CHUNKS);

        return Mono.from(chunksCollection.deleteMany(
                Filters.and(
                        Filters.exists("uploadId"),
                        Filters.ne("uploadId", null),
                        Filters.lt("createdAt", threshold))))
                .map(result -> result.getDeletedCount())
                .onErrorResume(e -> {
                    log.warn("清理孤儿 chunks 失败: {}", e.getMessage());
                    return Mono.just(0L);
                });
    }

    /**
     * Clean up expired multipart upload sessions (status=0 but timed out)
     */
    @Override
    public Mono<Long> cleanupExpiredUploads(int olderThanHours) {
        log.info("开始清理过期上传会话, olderThanHours={}", olderThanHours);

        Date threshold = Date.from(Instant.now().minus(Duration.ofHours(olderThanHours)));

        var uploadsCollection = driverContext.getMultipartUploadCollection();

        // 查找过期会话
        return Flux.from(uploadsCollection.find(
                Filters.and(
                        Filters.eq("status", 0),
                        Filters.lt("initiatedAt", threshold))))
                .map(doc -> doc.getString("uploadId"))
                .flatMap(uploadId -> abortExpiredUpload(uploadId)
                        .thenReturn(uploadId)
                        .onErrorResume(e -> {
                            log.warn("中止过期上传 {} 失败: {}", uploadId, e.getMessage());
                            return Mono.empty(); // Skip failed, continue processing next
                        }))
                .count()
                .doOnSuccess(count -> log.info("清理了 {} 个过期上传会话", count));
    }

    /**
     * 中止过期的上传，删除关联数据
     */
    private Mono<Void> abortExpiredUpload(String uploadId) {
        log.debug("中止过期上传: {}", uploadId);

        // 1. Delete chunks (requires iterating all databases)
        Mono<Void> deleteChunks;
        if (!dataClients.isEmpty()) {
            deleteChunks = Flux.range(0, mongoConfig.getDataDbCount())
                    .flatMap(index -> {
                        MongoDriverConfig.DataSourceConfig dsConfig = mongoConfig.getDataSourceConfig(index);
                        var db = dataClients.get(index).getDatabase(dsConfig.getDatabase());
                        var chunksCollection = db.getCollection(MongoDriverContext.COLLECTION_CHUNKS);
                        return Mono.from(chunksCollection.deleteMany(Filters.eq("uploadId", uploadId)));
                    })
                    .then();
        } else {
            var chunksCollection = driverContext.getMetaDatabase().getCollection(MongoDriverContext.COLLECTION_CHUNKS);
            deleteChunks = Mono.from(chunksCollection.deleteMany(Filters.eq("uploadId", uploadId))).then();
        }

        // 2. 删除 parts
        var partsCollection = driverContext.getMultipartPartCollection();
        Mono<Void> deleteParts = Mono.from(partsCollection.deleteMany(Filters.eq("uploadId", uploadId))).then();

        // 3. 更新会话状态为 3 (超时)
        var uploadsCollection = driverContext.getMultipartUploadCollection();
        Mono<Void> updateStatus = Mono.from(uploadsCollection.updateOne(
                Filters.eq("uploadId", uploadId),
                Updates.set("status", 3))).then();

        return deleteChunks
                .then(deleteParts)
                .then(updateStatus)
                .doOnSuccess(v -> log.debug("Aborted expired upload: {}", uploadId));
    }

    /**
     * Perform a full cleanup task
     */
    @Override
    public Mono<CleanupResult> performCleanup(int olderThanHours) {
        log.info("开始执行完整清理, olderThanHours={}", olderThanHours);

        return cleanupExpiredUploads(olderThanHours)
                .zipWith(cleanupOrphanChunks(olderThanHours))
                .map(tuple -> new CleanupResult(tuple.getT1(), tuple.getT2()))
                .doOnSuccess(result -> log.info("清理完成: {}", result));
    }
}
