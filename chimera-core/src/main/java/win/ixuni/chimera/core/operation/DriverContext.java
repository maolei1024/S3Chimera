package win.ixuni.chimera.core.operation;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.DriverConfig;

/**
 * Driver context interface
 * <p>
 * Provides shared dependencies and infrastructure needed by handlers to execute operations.
 * 每个驱动实现自己的上下文类。
 */
public interface DriverContext {

    /**
     * 获取驱动配置
     *
     * @return 驱动配置
     */
    DriverConfig getConfig();

    /**
     * Get the driver name
     *
     * @return 驱动实例名称
     */
    String getDriverName();

    /**
     * Get the driver type
     *
     * @return 驱动类型标识，如 "mysql", "memory"
     */
    String getDriverType();

    /**
     * Get the operation handler registry
     *
     * @return handler registry
     */
    OperationHandlerRegistry getHandlerRegistry();

    /**
     * 设置操作处理器注册表
     * <p>
     * Called during driver initialization to inject the handler registry.
     *
     * @param registry 处理器注册表
     */
    void setHandlerRegistry(OperationHandlerRegistry registry);

    /**
     * Execute an operation
     * <p>
     * Allows handlers to invoke other operations without directly depending on other handler instances.
     * 这提供了更好的解耦和可测试性。
     *
     * @param operation the operation instance
     * @param <O>       operation type
     * @param <R>       return type
     * @return operation result
     */
    default <O extends Operation<R>, R> Mono<R> execute(O operation) {
        return getHandlerRegistry().execute(operation, this);
    }
}
