package win.ixuni.chimera.driver.mysql.connection;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.ValidationDepth;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import win.ixuni.chimera.driver.mysql.config.MysqlDriverConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库连接健康检查器
 *
 * 职责：
 * 1. Verify all database connections on startup
 * 2. 运行时检测连接健康状态
 * 3. 跟踪连续失败次数，超过限制时触发致命错误
 */
@Slf4j
public class ConnectionHealthChecker {

    private final MysqlDriverConfig config;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicBoolean fatalErrorTriggered = new AtomicBoolean(false);

    public ConnectionHealthChecker(MysqlDriverConfig config) {
        this.config = config;
    }

    /**
     * Verify all database connections on startup
     *
     * @param metaPool 元数据库连接池
     * @param dataPools 数据库连接池列表
     * @return 验证成功返回 Mono.empty()，失败返回错误
     */
    public Mono<Void> validateConnectionsOnStartup(ConnectionPool metaPool, List<ConnectionPool> dataPools) {
        log.info("Validating database connections on startup...");

        Duration timeout = Duration.ofSeconds(config.getConnectionValidationTimeout());

        // 验证元数据库连接
        Mono<Void> validateMeta = validateConnection(metaPool, "meta-db", timeout)
                .doOnSuccess(v -> log.info("Meta database connection validated successfully"))
                .doOnError(e -> log.error("Failed to connect to meta database: {}", e.getMessage()));

        // Verify all database connections
        Mono<Void> validateData = Mono.empty();
        if (!dataPools.isEmpty()) {
            for (int i = 0; i < dataPools.size(); i++) {
                final int index = i;
                ConnectionPool pool = dataPools.get(i);
                validateData = validateData.then(
                        validateConnection(pool, "data-db-" + index, timeout)
                                .doOnSuccess(v -> log.info("Data database {} connection validated successfully", index))
                                .doOnError(e -> log.error("Failed to connect to data database {}: {}", index, e.getMessage()))
                );
            }
        }

        return validateMeta
                .then(validateData)
                .doOnSuccess(v -> log.info("All database connections validated successfully"))
                .onErrorMap(e -> new DatabaseConnectionException(
                        "Failed to establish database connection on startup. " +
                        "Please check database availability and configuration.", e));
    }

    /**
     * 验证单个连接池的连接
     */
    private Mono<Void> validateConnection(ConnectionPool pool, String poolName, Duration timeout) {
        return Mono.usingWhen(
                Mono.from(pool.create())
                        .timeout(timeout)
                        .doOnSubscribe(s -> log.debug("Acquiring connection from pool '{}' for validation", poolName)),
                connection -> Mono.from(connection.validate(ValidationDepth.REMOTE))
                        .timeout(timeout)
                        .<Void>flatMap(valid -> {
                            if (valid) {
                                log.debug("Connection validation successful for pool '{}'", poolName);
                                return Mono.empty();
                            } else {
                                return Mono.error(new DatabaseConnectionException(
                                        "Connection validation failed for pool '" + poolName + "'"));
                            }
                        }),
                connection -> Mono.from(connection.close())
        ).onErrorMap(e -> {
            if (e instanceof DatabaseConnectionException) {
                return e;
            }
            return new DatabaseConnectionException(
                    "Failed to validate connection for pool '" + poolName + "': " + e.getMessage(), e);
        }).then();
    }

    /**
     * 创建运行时重试策略
     * 在达到最大重试次数后触发致命错误
     *
     * @param operationName operation name (for logging)
     * @return 重试策略
     */
    public Retry createRuntimeRetrySpec(String operationName) {
        return Retry.backoff(config.getMaxReconnectAttempts(),
                        Duration.ofMillis(config.getReconnectInitialBackoff()))
                .maxBackoff(Duration.ofMillis(config.getReconnectMaxBackoff()))
                .filter(this::isRetryableException)
                .doBeforeRetry(signal -> {
                    int attempts = (int) signal.totalRetries() + 1;
                    log.warn("[{}] Retry attempt {}/{}: {}",
                            operationName, attempts, config.getMaxReconnectAttempts(),
                            signal.failure().getMessage());
                    consecutiveFailures.incrementAndGet();
                })
                .doAfterRetry(signal -> {
                    // 成功后重置连续失败计数
                    consecutiveFailures.set(0);
                })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    triggerFatalError(operationName, retrySignal.failure());
                    return new FatalDatabaseException(
                            String.format("[%s] Exhausted all %d retry attempts. Triggering application exit.",
                                    operationName, config.getMaxReconnectAttempts()),
                            retrySignal.failure());
                });
    }

    /**
     * Determine whether exception is retryable
     */
    public boolean isRetryableException(Throwable e) {
        if (e == null) return false;

        String msg = e.getMessage();
        if (msg == null) {
            // Check exception type when no message is present
            return isConnectionException(e);
        }

        msg = msg.toLowerCase();

        // 可重试的情况: 连接问题、超时、临时错误、死锁
        return msg.contains("connection") ||
               msg.contains("timeout") ||
               msg.contains("temporary") ||
               msg.contains("deadlock") ||
               msg.contains("lock wait") ||
               msg.contains("too many connections") ||
               msg.contains("communications link failure") ||
               msg.contains("connection reset") ||
               msg.contains("broken pipe") ||
               msg.contains("socket") ||
               msg.contains("network") ||
               isConnectionException(e);
    }

    /**
     * Check if this is a connection-related exception
     */
    private boolean isConnectionException(Throwable e) {
        String className = e.getClass().getName().toLowerCase();
        return className.contains("connection") ||
               className.contains("timeout") ||
               className.contains("socket") ||
               className.contains("network");
    }

    /**
     * 触发致命错误
     * Log error and set flag; caller should ensure application exits
     */
    private void triggerFatalError(String operationName, Throwable cause) {
        if (fatalErrorTriggered.compareAndSet(false, true)) {
            log.error("================================================================================");
            log.error("FATAL DATABASE ERROR - APPLICATION WILL EXIT");
            log.error("================================================================================");
            log.error("Operation: {}", operationName);
            log.error("Consecutive failures: {}", consecutiveFailures.get());
            log.error("Max reconnect attempts: {}", config.getMaxReconnectAttempts());
            log.error("Root cause: {}", cause.getMessage(), cause);
            log.error("================================================================================");
            log.error("The application cannot maintain database connectivity.");
            log.error("Please check:");
            log.error("  1. Database server availability");
            log.error("  2. Network connectivity");
            log.error("  3. Database credentials");
            log.error("  4. Connection pool settings");
            log.error("================================================================================");

            // 在单独的线程中触发系统退出，给日志刷新时间
            Thread exitThread = new Thread(() -> {
                try {
                    Thread.sleep(1000); // 给日志刷新1秒
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                log.error("Initiating application shutdown due to fatal database error...");
                System.exit(1);
            }, "FatalError-Exit-Thread");
            exitThread.setDaemon(false);
            exitThread.start();
        }
    }

    /**
     * Check if a fatal error has been triggered
     */
    public boolean isFatalErrorTriggered() {
        return fatalErrorTriggered.get();
    }

    /**
     * 获取连续失败次数
     */
    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    /**
     * Reset consecutive failure count (called on successful operation)
     */
    public void resetFailureCount() {
        consecutiveFailures.set(0);
    }

    /**
     * Database connection exception - for startup failures
     */
    public static class DatabaseConnectionException extends RuntimeException {
        public DatabaseConnectionException(String message) {
            super(message);
        }

        public DatabaseConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Fatal database exception - for exceeding max retries at runtime
     */
    public static class FatalDatabaseException extends RuntimeException {
        public FatalDatabaseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
