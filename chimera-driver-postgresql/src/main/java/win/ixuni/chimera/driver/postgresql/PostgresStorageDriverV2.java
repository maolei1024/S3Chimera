package win.ixuni.chimera.driver.postgresql;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.driver.CleanableDriver;
import win.ixuni.chimera.driver.postgresql.config.PostgresDriverConfig;
import win.ixuni.chimera.driver.postgresql.context.PostgresDriverContext;
import win.ixuni.chimera.driver.postgresql.handler.bucket.*;
import win.ixuni.chimera.driver.postgresql.handler.multipart.*;
import win.ixuni.chimera.driver.postgresql.handler.object.*;
import win.ixuni.chimera.driver.postgresql.schema.PostgresSchemaInitializer;
import win.ixuni.chimera.driver.postgresql.sharding.ShardingRouter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * PostgreSQL storage driver V2
 * 
 * Implements the new command-pattern architecture.
 * Each operation is handled by an independent handler; the driver only initializes and composes them.
 */
@Slf4j
public class PostgresStorageDriverV2 extends AbstractStorageDriverV2 implements CleanableDriver {

    private final DriverConfig config;

    @Getter
    private PostgresDriverContext driverContext;

    private PostgresDriverConfig postgresConfig;
    private ConnectionPool metaConnectionPool;
    private final List<ConnectionPool> dataConnectionPools = new ArrayList<>();

    public PostgresStorageDriverV2(DriverConfig config) {
        this.config = config;
    }

    @Override
    public String getDriverType() {
        return PostgresDriverFactory.DRIVER_TYPE;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public Mono<Void> initialize() {
        return Mono.fromRunnable(this::parseConfig)
                .then(Mono.fromRunnable(this::createConnectionPools))
                .then(Mono.fromRunnable(this::buildDriverContext))
                .then(Mono.fromRunnable(this::registerHandlers))
                .then(Mono.defer(this::initializeSchema))
                .doOnSuccess(v -> log.info("PostgreSQL 驱动 V2 '{}' 初始化完成，共 {} 个处理器",
                        getDriverName(), getHandlerRegistry().size()))
                .doOnError(e -> log.error("PostgreSQL 驱动 V2 '{}' 初始化失败", getDriverName(), e));
    }

    private void parseConfig() {
        String url = config.getString("url", "");
        String username = config.getString("username", "");
        String password = config.getString("password", "");

        List<PostgresDriverConfig.DataSourceConfig> dataSources = parseDataSources();

        int tableShardCount = config.getInt("table-shard-count", 16);
        int totalHashSlots = config.getInt("total-hash-slots", 1024);
        int chunkSize = parseDataSize(config.getString("chunk-size", "4MB"));
        int poolSize = config.getInt("pool-size", 10);
        int writeConcurrency = config.getInt("write-concurrency", 2);
        int readConcurrency = config.getInt("read-concurrency", 4);
        boolean autoCreateSchema = config.getBoolean("auto-create-schema", true);

        if (url.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL 驱动需要配置 'url'");
        }

        this.postgresConfig = PostgresDriverConfig.builder()
                .metaUrl(url)
                .metaUsername(username.isEmpty() ? null : username)
                .metaPassword(password.isEmpty() ? null : password)
                .dataSources(dataSources)
                .tableShardCount(tableShardCount)
                .totalHashSlots(totalHashSlots)
                .chunkSize(chunkSize)
                .poolSize(poolSize)
                .writeConcurrency(writeConcurrency)
                .readConcurrency(readConcurrency)
                .autoCreateSchema(autoCreateSchema)
                .build();

        log.info("PostgreSQL 驱动 V2 配置解析完成: url={}, chunkSize={}KB, poolSize={}",
                url, chunkSize / 1024, poolSize);
    }

    @SuppressWarnings("unchecked")
    private List<PostgresDriverConfig.DataSourceConfig> parseDataSources() {
        List<PostgresDriverConfig.DataSourceConfig> result = new ArrayList<>();
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
        // 情况2: Spring Boot 将 YAML 列表解析为 Map 格式
        else if (dataSourcesObj instanceof Map<?, ?> map) {
            int index = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                Object item = entry.getValue();
                parseDataSourceItem(item, index++, result);
            }
        }

        log.info("[PARSE_DS] 解析到 {} 个数据源", result.size());
        return result.isEmpty() ? null : result;
    }

    @SuppressWarnings("unchecked")
    private void parseDataSourceItem(Object item, int index, List<PostgresDriverConfig.DataSourceConfig> result) {
        if (item instanceof Map<?, ?> map) {
            Map<String, Object> dsMap = (Map<String, Object>) map;
            String dsUrl = getMapString(dsMap, "url", "");
            if (!dsUrl.isEmpty()) {
                result.add(PostgresDriverConfig.DataSourceConfig.builder()
                        .name(getMapString(dsMap, "name", "data-" + index))
                        .url(dsUrl)
                        .username(getMapString(dsMap, "username", null))
                        .password(getMapString(dsMap, "password", null))
                        .build());
            }
        }
    }

    private String getMapString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
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

    private void createConnectionPools() {
        this.metaConnectionPool = createConnectionPool(
                postgresConfig.getMetaUrl(), postgresConfig.getMetaUsername(),
                postgresConfig.getMetaPassword(), "meta");

        int dataDbCount = postgresConfig.getDataDbCount();
        for (int i = 0; i < dataDbCount; i++) {
            PostgresDriverConfig.DataSourceConfig dsConfig = postgresConfig.getDataSourceConfig(i);
            if (dsConfig != null) {
                dataConnectionPools.add(createConnectionPool(
                        dsConfig.getUrl(), dsConfig.getUsername(),
                        dsConfig.getPassword(), dsConfig.getName()));
            }
        }
    }

    private ConnectionPool createConnectionPool(String url, String username, String password, String name) {
        ConnectionFactoryOptions baseOptions = ConnectionFactoryOptions.parse(url);
        ConnectionFactoryOptions.Builder optionsBuilder = ConnectionFactoryOptions.builder().from(baseOptions);

        if (username != null && !username.isEmpty())
            optionsBuilder.option(USER, username);
        if (password != null && !password.isEmpty())
            optionsBuilder.option(PASSWORD, password);

        ConnectionFactory connectionFactory = ConnectionFactories.get(optionsBuilder.build());

        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .name("chimera-pg-" + name)
                .initialSize(2)
                .maxSize(postgresConfig.getPoolSize())
                .maxIdleTime(Duration.ofMinutes(10))
                .build();

        return new ConnectionPool(poolConfig);
    }

    private void buildDriverContext() {
        ShardingRouter shardingRouter = new ShardingRouter(postgresConfig);

        this.driverContext = PostgresDriverContext.builder()
                .config(config)
                .postgresConfig(postgresConfig)
                .metaConnectionPool(metaConnectionPool)
                .dataConnectionPools(dataConnectionPools)
                .shardingRouter(shardingRouter)
                .build();
    }

    private void registerHandlers() {
        // S3Bucket 处理器 (4)
        getHandlerRegistry().register(new PostgresCreateBucketHandler());
        getHandlerRegistry().register(new PostgresDeleteBucketHandler());
        getHandlerRegistry().register(new PostgresBucketExistsHandler());
        getHandlerRegistry().register(new PostgresListBucketsHandler());

        // Object 处理器 (9)
        getHandlerRegistry().register(new PostgresHeadObjectHandler());
        getHandlerRegistry().register(new PostgresGetObjectHandler());
        getHandlerRegistry().register(new PostgresDeleteObjectHandler());
        getHandlerRegistry().register(new PostgresPutObjectHandler());
        getHandlerRegistry().register(new PostgresCopyObjectHandler());
        getHandlerRegistry().register(new PostgresDeleteObjectsHandler());
        getHandlerRegistry().register(new PostgresListObjectsHandler());
        getHandlerRegistry().register(new PostgresListObjectsV2Handler());
        getHandlerRegistry().register(new PostgresGetObjectRangeHandler());

        // Multipart 处理器 (6)
        getHandlerRegistry().register(new PostgresCreateMultipartUploadHandler());
        getHandlerRegistry().register(new PostgresUploadPartHandler());
        getHandlerRegistry().register(new PostgresCompleteMultipartUploadHandler());
        getHandlerRegistry().register(new PostgresAbortMultipartUploadHandler());
        getHandlerRegistry().register(new PostgresListPartsHandler());
        getHandlerRegistry().register(new PostgresListMultipartUploadsHandler());

        log.info("注册 {} 个操作处理器", getHandlerRegistry().size());

        // 注入 handlerRegistry 到 context
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private Mono<Void> initializeSchema() {
        if (!postgresConfig.isAutoCreateSchema()) {
            return Mono.empty();
        }

        List<ConnectionFactory> dataFactories = new ArrayList<>(dataConnectionPools);
        PostgresSchemaInitializer initializer = new PostgresSchemaInitializer(
                postgresConfig, metaConnectionPool, dataFactories);
        return initializer.initialize();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("关闭 PostgreSQL 驱动 V2: {}", getDriverName());

        List<Mono<Void>> disposeTasks = new ArrayList<>();
        if (metaConnectionPool != null) {
            disposeTasks.add(Mono.fromRunnable(() -> metaConnectionPool.dispose()));
        }
        for (ConnectionPool pool : dataConnectionPools) {
            disposeTasks.add(Mono.fromRunnable(pool::dispose));
        }

        return Mono.when(disposeTasks);
    }

    // ==================== CleanableDriver 实现 ====================

    /**
     * Clean up orphan chunks (with upload_id but creation time exceeds threshold)
     * 
     * PostgreSQL uses declarative partitioning; operations on parent table t_chunk auto-route to all partitions
     * In multi-database mode, need to iterate all databases
     */
    @Override
    public Mono<Long> cleanupOrphanChunks(int olderThanHours) {
        log.info("开始清理孤儿 chunks, olderThanHours={}", olderThanHours);

        // PostgreSQL uses INTERVAL syntax
        String sql = "DELETE FROM t_chunk WHERE upload_id IS NOT NULL " +
                "AND created_at < NOW() - INTERVAL '" + olderThanHours + " hours'";

        // If there are multiple databases, iterate all databases
        if (!dataConnectionPools.isEmpty()) {
            return Flux.range(0, dataConnectionPools.size())
                    .flatMap(index -> driverContext.executeUpdate(dataConnectionPools.get(index), sql)
                            .doOnSuccess(count -> {
                                if (count > 0) {
                                    log.debug("从数据库 {} 清理了 {} 个孤儿 chunks", index, count);
                                }
                            })
                            .onErrorResume(e -> {
                                log.warn("清理数据库 {} 中的孤儿 chunks 失败: {}", index, e.getMessage());
                                return Mono.just(0L);
                            }))
                    .reduce(0L, Long::sum)
                    .doOnSuccess(count -> {
                        if (count > 0) {
                            log.info("清理了 {} 个孤儿 chunks", count);
                        }
                    });
        } else {
            // 单库模式：chunk 表在 meta 库
            return driverContext.executeUpdate(driverContext.getMetaConnectionPool(), sql)
                    .doOnSuccess(count -> {
                        if (count > 0) {
                            log.info("清理了 {} 个孤儿 chunks", count);
                        }
                    })
                    .onErrorResume(e -> {
                        log.warn("清理孤儿 chunks 失败: {}", e.getMessage());
                        return Mono.just(0L);
                    });
        }
    }

    /**
     * Clean up expired multipart upload sessions (status=0 but timed out)
     */
    @Override
    public Mono<Long> cleanupExpiredUploads(int olderThanHours) {
        log.info("开始清理过期上传会话, olderThanHours={}", olderThanHours);

        // 1. 查找过期会话
        String findExpiredSql = "SELECT upload_id, bucket_name, object_key FROM t_multipart_upload " +
                "WHERE status = 0 AND initiated_at < NOW() - INTERVAL '" + olderThanHours + " hours'";

        return driverContext.executeQueryMapped(
                driverContext.getMetaConnectionPool(),
                findExpiredSql,
                row -> row.get("upload_id", String.class))
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
     * In multi-database mode, need to iterate all databases to delete chunks
     */
    private Mono<Void> abortExpiredUpload(String uploadId) {
        log.debug("中止过期上传: {}", uploadId);

        // 1. Delete from t_chunk (requires iterating all databases)
        String deleteChunksSql = "DELETE FROM t_chunk WHERE upload_id = $1";
        Mono<Void> deleteChunks;

        if (!dataConnectionPools.isEmpty()) {
            deleteChunks = Flux.range(0, dataConnectionPools.size())
                    .flatMap(index -> driverContext.executeUpdate(dataConnectionPools.get(index), deleteChunksSql,
                            uploadId))
                    .then();
        } else {
            // 单库模式
            deleteChunks = driverContext.executeUpdate(driverContext.getMetaConnectionPool(), deleteChunksSql, uploadId)
                    .then();
        }

        // 2. 从 t_multipart_part 删除
        String deletePartsSql = "DELETE FROM t_multipart_part WHERE upload_id = $1";

        // 3. 更新 t_multipart_upload 状态为 3 (超时)
        String updateStatusSql = "UPDATE t_multipart_upload SET status = 3 WHERE upload_id = $1";

        return deleteChunks
                .then(driverContext.executeUpdate(driverContext.getMetaConnectionPool(), deletePartsSql, uploadId))
                .then(driverContext.executeUpdate(driverContext.getMetaConnectionPool(), updateStatusSql, uploadId))
                .then()
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
