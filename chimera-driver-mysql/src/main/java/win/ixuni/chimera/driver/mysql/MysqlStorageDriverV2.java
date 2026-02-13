package win.ixuni.chimera.driver.mysql;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.driver.AbstractStorageDriverV2;
import win.ixuni.chimera.core.driver.CleanableDriver;
import win.ixuni.chimera.driver.mysql.config.MysqlDriverConfig;
import win.ixuni.chimera.driver.mysql.connection.ConnectionHealthChecker;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.bucket.MysqlBucketExistsHandler;
import win.ixuni.chimera.driver.mysql.handler.bucket.MysqlCreateBucketHandler;
import win.ixuni.chimera.driver.mysql.handler.bucket.MysqlDeleteBucketHandler;
import win.ixuni.chimera.driver.mysql.handler.bucket.MysqlListBucketsHandler;
import win.ixuni.chimera.driver.mysql.handler.object.*;
import win.ixuni.chimera.driver.mysql.schema.MysqlSchemaInitializer;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * MySQL storage driver V2
 * <p>
 * Implements the new command-pattern architecture.
 * Each operation is handled by an independent handler; the driver only initializes and composes them.
 */
@Slf4j
public class MysqlStorageDriverV2 extends AbstractStorageDriverV2 implements CleanableDriver {

    private final DriverConfig config;

    @Getter
    private MysqlDriverContext driverContext;

    private MysqlDriverConfig mysqlConfig;
    private ConnectionPool metaConnectionPool;
    private final List<ConnectionPool> dataConnectionPools = new ArrayList<>();

    public MysqlStorageDriverV2(DriverConfig config) {
        this.config = config;
    }

    @Override
    public String getDriverType() {
        return SqlDriverFactory.DRIVER_TYPE;
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
                .then(Mono.defer(this::validateConnections))
                .then(Mono.defer(this::initializeSchema))
                .doOnSuccess(v -> log.info("MySQL driver V2 '{}' initialized with {} handlers",
                        getDriverName(), getHandlerRegistry().size()))
                .doOnError(e -> log.error("MySQL driver V2 '{}' initialization failed", getDriverName(), e));
    }

    private void parseConfig() {
        String url = config.getString("url", "");
        String username = config.getString("username", "");
        String password = config.getString("password", "");

        // 解析数据源配置
        List<MysqlDriverConfig.DataSourceConfig> dataSources = parseDataSources();
        String[] dataUrls = null;
        if (dataSources == null || dataSources.isEmpty()) {
            String dataUrlsStr = config.getString("data-urls", "");
            dataUrls = dataUrlsStr.isEmpty() ? new String[0] : dataUrlsStr.split(",");
        }

        int tableShardCount = config.getInt("table-shard-count", 16);
        int totalHashSlots = config.getInt("total-hash-slots", 1024);
        int chunkSize = parseDataSize(config.getString("chunk-size", "4MB"));
        int poolSize = config.getInt("pool-size", 10);
        int writeConcurrency = config.getInt("write-concurrency", 2);
        int readConcurrency = config.getInt("read-concurrency", 4);
        boolean autoCreateSchema = config.getBoolean("auto-create-schema", true);

        if (url.isEmpty()) {
            throw new IllegalArgumentException("MySQL driver requires 'url' configuration");
        }

        this.mysqlConfig = MysqlDriverConfig.builder()
                .metaUrl(url)
                .metaUsername(username)
                .metaPassword(password)
                .dataSources(dataSources)
                .dataUrls(dataUrls != null && dataUrls.length > 0 ? dataUrls : null)
                .dataUsername(username)
                .dataPassword(password)
                .tableShardCount(tableShardCount)
                .totalHashSlots(totalHashSlots)
                .chunkSize(chunkSize)
                .poolSize(poolSize)
                .writeConcurrency(writeConcurrency)
                .readConcurrency(readConcurrency)
                .autoCreateSchema(autoCreateSchema)
                .build();

        log.info("MySQL driver V2 config parsed: url={}, chunkSize={}KB, poolSize={}",
                url, chunkSize / 1024, poolSize);
    }

    @SuppressWarnings("unchecked")
    private List<MysqlDriverConfig.DataSourceConfig> parseDataSources() {
        List<MysqlDriverConfig.DataSourceConfig> result = new ArrayList<>();
        Object dataSourcesObj = config.getProperty("data-sources", null);

        // Debug log: print config object info
        if (dataSourcesObj != null) {
            log.info("[PARSE_DS] data-sources type={}, value={}",
                    dataSourcesObj.getClass().getName(), dataSourcesObj);
        } else {
            log.warn("[PARSE_DS] data-sources is NULL - will use single-database mode");
            return null;
        }

        // 情况1: 标准 List 格式
        if (dataSourcesObj instanceof List<?> list) {
            log.info("[PARSE_DS] data-sources is a List with {} items", list.size());
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                parseDataSourceItem(item, i, result);
            }
        }
        // Case 2: Spring Boot parses YAML lists as Map format (key is index)
        else if (dataSourcesObj instanceof Map<?, ?> map) {
            log.info("[PARSE_DS] data-sources is a Map with {} entries (Spring Boot list-to-map conversion)",
                    map.size());
            int index = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                Object item = entry.getValue();
                parseDataSourceItem(item, index++, result);
            }
        } else {
            log.warn("[PARSE_DS] data-sources has unexpected type: {}. Expected List or Map.",
                    dataSourcesObj.getClass().getName());
        }

        log.info("[PARSE_DS] Parsed {} data sources total", result.size());
        return result.isEmpty() ? null : result;
    }

    @SuppressWarnings("unchecked")
    private void parseDataSourceItem(Object item, int index, List<MysqlDriverConfig.DataSourceConfig> result) {
        log.info("[PARSE_DS] Item[{}] type={}, value={}", index,
                item != null ? item.getClass().getName() : "null", item);

        if (item instanceof Map<?, ?> map) {
            Map<String, Object> dsMap = (Map<String, Object>) map;
            String dsUrl = getMapString(dsMap, "url", "");
            log.info("[PARSE_DS] Item[{}] url={}", index, dsUrl);
            if (!dsUrl.isEmpty()) {
                String name = getMapString(dsMap, "name", "data-" + index);
                result.add(MysqlDriverConfig.DataSourceConfig.builder()
                        .name(name)
                        .url(dsUrl)
                        .username(getMapString(dsMap, "username", ""))
                        .password(getMapString(dsMap, "password", ""))
                        .build());
                log.info("[PARSE_DS] Added data source: name={}, url={}", name, dsUrl);
            } else {
                log.warn("[PARSE_DS] Item[{}] has empty URL, skipped", index);
            }
        } else {
            log.warn("[PARSE_DS] Item[{}] is not a Map, type={}", index,
                    item != null ? item.getClass().getName() : "null");
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

    private void createConnectionPools() {
        this.metaConnectionPool = createConnectionPool(
                mysqlConfig.getMetaUrl(), mysqlConfig.getMetaUsername(),
                mysqlConfig.getMetaPassword(), "meta");

        int dataDbCount = mysqlConfig.getDataDbCount();
        for (int i = 0; i < dataDbCount; i++) {
            MysqlDriverConfig.DataSourceConfig dsConfig = mysqlConfig.getDataSourceConfig(i);
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
        optionsBuilder.option(Option.valueOf("tcpKeepAlive"), true);

        // Enable prepared statements and binary protocol to avoid bandwidth doubling from text-protocol BLOB encoding
        // Text protocol hex/base64-encodes binary data, causing ~100% transfer overhead
        optionsBuilder.option(Option.valueOf("useServerPrepStmts"), true);
        optionsBuilder.option(Option.valueOf("cachePrepStmts"), true);
        optionsBuilder.option(Option.valueOf("prepStmtCacheSize"), 250);
        optionsBuilder.option(Option.valueOf("prepStmtCacheSqlLimit"), 2048);

        ConnectionFactory connectionFactory = ConnectionFactories.get(optionsBuilder.build());

        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .name("chimera-v2-" + name)
                .initialSize(2)
                .maxSize(mysqlConfig.getPoolSize())
                .maxIdleTime(Duration.ofMinutes(10))
                .build();

        return new ConnectionPool(poolConfig);
    }

    private void buildDriverContext() {
        ShardingRouter shardingRouter = new ShardingRouter(mysqlConfig);
        ConnectionHealthChecker healthChecker = new ConnectionHealthChecker(mysqlConfig);

        this.driverContext = MysqlDriverContext.builder()
                .config(config)
                .mysqlConfig(mysqlConfig)
                .metaConnectionPool(metaConnectionPool)
                .dataConnectionPools(dataConnectionPools)
                .shardingRouter(shardingRouter)
                .connectionHealthChecker(healthChecker)
                .build();
    }

    private void registerHandlers() {
        // Bucket handlers (4)
        getHandlerRegistry().register(new MysqlCreateBucketHandler());
        getHandlerRegistry().register(new MysqlDeleteBucketHandler());
        getHandlerRegistry().register(new MysqlBucketExistsHandler());
        getHandlerRegistry().register(new MysqlListBucketsHandler());

        // Object handlers (9)
        getHandlerRegistry().register(new MysqlHeadObjectHandler());
        getHandlerRegistry().register(new MysqlGetObjectHandler());
        getHandlerRegistry().register(new MysqlDeleteObjectHandler());
        getHandlerRegistry().register(new MysqlPutObjectHandler());
        getHandlerRegistry().register(new MysqlCopyObjectHandler());
        getHandlerRegistry().register(new MysqlDeleteObjectsHandler());
        getHandlerRegistry().register(new MysqlListObjectsHandler());
        getHandlerRegistry().register(new MysqlListObjectsV2Handler());
        getHandlerRegistry().register(new MysqlGetObjectRangeHandler());

        // Multipart handlers (6)
        getHandlerRegistry()
                .register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlCreateMultipartUploadHandler());
        getHandlerRegistry().register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlUploadPartHandler());
        getHandlerRegistry()
                .register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlCompleteMultipartUploadHandler());
        getHandlerRegistry()
                .register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlAbortMultipartUploadHandler());
        getHandlerRegistry().register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlListPartsHandler());
        getHandlerRegistry()
                .register(new win.ixuni.chimera.driver.mysql.handler.multipart.MysqlListMultipartUploadsHandler());

        log.info("Registered {} operation handlers", getHandlerRegistry().size());

        // Inject handlerRegistry into context so handlers can invoke other operations via context.execute()
        driverContext.setHandlerRegistry(getHandlerRegistry());
    }

    private Mono<Void> validateConnections() {
        ConnectionHealthChecker healthChecker = driverContext.getConnectionHealthChecker();
        return healthChecker.validateConnectionsOnStartup(metaConnectionPool, dataConnectionPools);
    }

    private Mono<Void> initializeSchema() {
        if (!mysqlConfig.isAutoCreateSchema()) {
            return Mono.empty();
        }

        List<ConnectionFactory> dataFactories = new ArrayList<>(dataConnectionPools);
        MysqlSchemaInitializer initializer = new MysqlSchemaInitializer(
                mysqlConfig, metaConnectionPool, dataFactories);
        return initializer.initialize();
    }

    @Override
    public Mono<Void> shutdown() {
        log.info("Shutting down MySQL driver V2: {}", getDriverName());

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
     * MySQL uses manual sharding, requiring iteration of all shard tables
     * In multi-database mode, also need to iterate all databases
     */
    @Override
    public Mono<Long> cleanupOrphanChunks(int olderThanHours) {
        log.info("开始清理孤儿 chunks, olderThanHours={}", olderThanHours);

        // Get connection pool list (single-DB mode: meta only; multi-DB mode: all data pools)
        List<ConnectionPool> poolsToClean = dataConnectionPools.isEmpty()
                ? List.of(metaConnectionPool)
                : dataConnectionPools;

        return Flux.fromIterable(poolsToClean)
                .index()
                .flatMap(tuple -> {
                    int poolIndex = tuple.getT1().intValue();
                    ConnectionPool pool = tuple.getT2();
                    return cleanupOrphanChunksInPool(pool, poolIndex, olderThanHours);
                })
                .reduce(0L, Long::sum)
                .doOnSuccess(count -> {
                    if (count > 0) {
                        log.info("清理了 {} 个孤儿 chunks", count);
                    }
                });
    }

    private Mono<Long> cleanupOrphanChunksInPool(ConnectionPool pool, int poolIndex, int olderThanHours) {
        return Flux.range(0, mysqlConfig.getTableShardCount())
                .flatMap(shardIndex -> {
                    String chunkTable = "t_chunk_" + String.format("%02x", shardIndex);
                    // MySQL uses DATE_SUB syntax
                    String sql = "DELETE FROM " + chunkTable +
                            " WHERE upload_id IS NOT NULL " +
                            " AND created_at < DATE_SUB(NOW(), INTERVAL ? HOUR)";

                    return driverContext.executeUpdate(pool, sql, olderThanHours)
                            .doOnSuccess(count -> {
                                if (count > 0) {
                                    log.debug("从数据库 {} 的 {} 清理了 {} 个孤儿 chunks", poolIndex, chunkTable, count);
                                }
                            })
                            .onErrorResume(e -> {
                                log.warn("清理数据库 {} 的 {} 中的孤儿 chunks 失败: {}", poolIndex, chunkTable, e.getMessage());
                                return Mono.just(0L);
                            });
                })
                .reduce(0L, Long::sum);
    }

    /**
     * Clean up expired multipart upload sessions (status=0 but timed out)
     */
    @Override
    public Mono<Long> cleanupExpiredUploads(int olderThanHours) {
        log.info("开始清理过期上传会话, olderThanHours={}", olderThanHours);

        // 查找过期会话
        String findExpiredSql = "SELECT upload_id FROM t_multipart_upload " +
                "WHERE status = 0 AND initiated_at < DATE_SUB(NOW(), INTERVAL ? HOUR)";

        return driverContext.executeQueryMapped(
                driverContext.getMetaConnectionPool(),
                findExpiredSql,
                row -> row.get("upload_id", String.class),
                olderThanHours)
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
     * Abort expired uploads and delete associated data from all shard tables
     * In multi-database mode, need to iterate all databases
     */
    private Mono<Void> abortExpiredUpload(String uploadId) {
        log.debug("中止过期上传: {}", uploadId);

        // 获取需要操作的连接池列表
        List<ConnectionPool> poolsToClean = dataConnectionPools.isEmpty()
                ? List.of(metaConnectionPool)
                : dataConnectionPools;

        // 1. Delete from all shard chunk tables (iterate all shard tables across all databases)
        Mono<Void> deleteChunks = Flux.fromIterable(poolsToClean)
                .flatMap(pool -> Flux.range(0, mysqlConfig.getTableShardCount())
                        .flatMap(shardIndex -> {
                            String chunkTable = "t_chunk_" + String.format("%02x", shardIndex);
                            String sql = "DELETE FROM " + chunkTable + " WHERE upload_id = ?";
                            return driverContext.executeUpdate(pool, sql, uploadId);
                        }))
                .then();

        // 2. 从 t_multipart_part 删除
        String deletePartsSql = "DELETE FROM t_multipart_part WHERE upload_id = ?";

        // 3. 更新 t_multipart_upload 状态为 3 (超时)
        String updateStatusSql = "UPDATE t_multipart_upload SET status = 3 WHERE upload_id = ?";

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
