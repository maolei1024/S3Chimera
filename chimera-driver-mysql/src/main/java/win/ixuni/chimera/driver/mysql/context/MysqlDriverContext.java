package win.ixuni.chimera.driver.mysql.context;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Row;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;
import win.ixuni.chimera.driver.mysql.SqlDriverFactory;
import win.ixuni.chimera.driver.mysql.config.MysqlDriverConfig;
import win.ixuni.chimera.driver.mysql.connection.ConnectionHealthChecker;
import win.ixuni.chimera.driver.mysql.sharding.ShardingRouter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

/**
 * MySQL 驱动上下文
 * <p>
 * Provides shared dependencies and infrastructure needed by handlers to execute operations.
 */
@Slf4j
@Getter
@Builder
public class MysqlDriverContext implements DriverContext {

    /**
     * Driver configuration
     */
    private final DriverConfig config;

    /**
     * MySQL 特定配置
     */
    private final MysqlDriverConfig mysqlConfig;

    /**
     * 元数据库连接池
     */
    private final ConnectionPool metaConnectionPool;

    /**
     * 数据库连接池列表
     */
    private final List<ConnectionPool> dataConnectionPools;

    /**
     * Shard router
     */
    private final ShardingRouter shardingRouter;

    /**
     * 连接健康检查器
     */
    private final ConnectionHealthChecker connectionHealthChecker;

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
        return SqlDriverFactory.DRIVER_TYPE;
    }

    /**
     * Get data connection pool (by shard routing)
     *
     * @param bucketName Bucket 名称
     * @param objectKey  对象 Key
     * @return the corresponding connection pool
     */
    public ConnectionPool getDataConnectionPool(String bucketName, String objectKey) {
        if (dataConnectionPools.isEmpty()) {
            return metaConnectionPool; // 单库模式
        }
        int dbIndex = shardingRouter.getDataDbIndex(bucketName, objectKey);
        return dataConnectionPools.get(dbIndex);
    }

    /**
     * Get the default data connection pool (for cleanup tasks and other non-routed scenarios)
     */
    public ConnectionPool getDefaultDataConnectionPool() {
        if (dataConnectionPools.isEmpty()) {
            return metaConnectionPool; // 单库模式
        }
        return dataConnectionPools.get(0);
    }

    /**
     * 执行查询（无参数）
     */
    public Flux<Row> executeQuery(ConnectionPool pool, String sql) {
        return Flux.usingWhen(
                Mono.from(pool.create()),
                connection -> Flux.from(connection.createStatement(sql).execute())
                        .flatMap(result -> Flux.from(result.map((row, meta) -> row))),
                connection -> Mono.from(connection.close()));
    }

    /**
     * 执行查询（带参数）
     */
    public Flux<Row> executeQuery(ConnectionPool pool, String sql, Object... params) {
        return Flux.usingWhen(
                Mono.from(pool.create()),
                connection -> {
                    var stmt = connection.createStatement(sql);
                    for (int i = 0; i < params.length; i++) {
                        stmt.bind(i, params[i]);
                    }
                    return Flux.from(stmt.execute())
                            .flatMap(result -> Flux.from(result.map((row, meta) -> row)));
                },
                connection -> Mono.from(connection.close()));
    }

    /**
     * 执行更新（INSERT/UPDATE/DELETE）
     */
    public Mono<Long> executeUpdate(ConnectionPool pool, String sql, Object... params) {
        return Mono.usingWhen(
                Mono.from(pool.create()),
                connection -> {
                    var stmt = connection.createStatement(sql);
                    for (int i = 0; i < params.length; i++) {
                        stmt.bind(i, params[i]);
                    }
                    return Mono.from(stmt.execute())
                            .flatMap(result -> Mono.from(result.getRowsUpdated()));
                },
                connection -> Mono.from(connection.close()));
    }

    /**
     * Execute query and map results (without parameters)
     * Row 在连接内被消费，避免 ByteBuf 生命周期问题
     */
    public <T> Flux<T> executeQueryMapped(ConnectionPool pool, String sql, Function<Row, T> mapper) {
        return Flux.usingWhen(
                Mono.from(pool.create()),
                connection -> Flux.from(connection.createStatement(sql).execute())
                        .flatMap(result -> Flux.from(result.map((row, meta) -> mapper.apply(row)))),
                connection -> Mono.from(connection.close()));
    }

    /**
     * Execute query and map results (with parameters)
     * Row 在连接内被消费，避免 ByteBuf 生命周期问题
     */
    public <T> Flux<T> executeQueryMapped(ConnectionPool pool, String sql, Function<Row, T> mapper, Object... params) {
        return Flux.usingWhen(
                Mono.from(pool.create()),
                connection -> {
                    var stmt = connection.createStatement(sql);
                    for (int i = 0; i < params.length; i++) {
                        if (params[i] == null) {
                            stmt.bindNull(i, Object.class);
                        } else {
                            stmt.bind(i, params[i]);
                        }
                    }
                    return Flux.from(stmt.execute())
                            .flatMap(result -> Flux.from(result.map((row, meta) -> mapper.apply(row))));
                },
                connection -> Mono.from(connection.close()));
    }

    /**
     * LocalDateTime 转 Instant
     */
    public Instant toInstant(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
    }
}
