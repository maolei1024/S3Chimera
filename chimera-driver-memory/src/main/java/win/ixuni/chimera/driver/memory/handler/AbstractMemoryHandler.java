package win.ixuni.chimera.driver.memory.handler;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.Operation;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

/**
 * Memory Handler 抽象基类
 * <p>
 * 提供类型安全的 Context 访问，子类无需手动强制转换。
 *
 * @param <O> 操作类型
 * @param <R> 返回类型
 */
public abstract class AbstractMemoryHandler<O extends Operation<R>, R> implements OperationHandler<O, R> {

    @Override
    public final Mono<R> handle(O operation, DriverContext context) {
        if (!(context instanceof MemoryDriverContext)) {
            return Mono.error(new IllegalArgumentException(
                    "Expected MemoryDriverContext but got: " + context.getClass().getName()));
        }
        return doHandle(operation, (MemoryDriverContext) context);
    }

    /**
     * 子类实现的处理方法，接收类型安全的 MemoryDriverContext
     *
     * @param operation 操作
     * @param context   Memory 驱动上下文
     * @return operation result
     */
    protected abstract Mono<R> doHandle(O operation, MemoryDriverContext context);
}
