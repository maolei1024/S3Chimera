package win.ixuni.chimera.core.operation;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;

import java.util.Collections;
import java.util.Set;

/**
 * Operation handler interface
 * <p>
 * Each driver provides the corresponding handler implementation for its supported operations.
 * Uses generics to ensure type safety.
 *
 * @param <O> 操作类型
 * @param <R> 返回类型
 */
public interface OperationHandler<O extends Operation<R>, R> {

    /**
     * Handle the operation
     *
     * @param operation the operation instance
     * @param context   驱动上下文
     * @return operation result
     */
    Mono<R> handle(O operation, DriverContext context);

    /**
     * 获取此处理器支持的操作类型
     *
     * @return 操作类类型
     */
    Class<O> getOperationType();

    /**
     * 获取此处理器贡献的能力集合
     * <p>
     * The driver total capability is the union of all registered handler capabilities.
     * Subclasses should override this method to declare their provided capabilities.
     *
     * @return 能力集合，默认为空
     */
    default Set<Capability> getProvidedCapabilities() {
        return Collections.emptySet();
    }
}
