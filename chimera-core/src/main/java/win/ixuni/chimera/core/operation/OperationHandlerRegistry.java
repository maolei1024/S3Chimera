package win.ixuni.chimera.core.operation;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Operation handler registry
 * <p>
 * Manages the mapping between operations and their handlers.
 * 驱动初始化时注册支持的处理器，运行时根据操作类型查找并执行。
 * <p>
 * Supports interceptor chains, allowing common logic before and after handler execution.
 */
@Slf4j
public class OperationHandlerRegistry {

    private final Map<Class<?>, OperationHandler<?, ?>> handlers = new ConcurrentHashMap<>();
    private final List<HandlerInterceptor> interceptors = new CopyOnWriteArrayList<>();

    /**
     * Register an operation handler
     *
     * @param handler 处理器实例
     * @param <O>     操作类型
     * @param <R>     返回类型
     */
    public <O extends Operation<R>, R> void register(OperationHandler<O, R> handler) {
        Class<O> operationType = handler.getOperationType();
        handlers.put(operationType, handler);
        log.debug("Registered handler for operation: {}", operationType.getSimpleName());
    }

    /**
     * 添加拦截器
     * <p>
     * 拦截器按 order 排序，order 越小越先执行。
     *
     * @param interceptor 拦截器实例
     */
    public void addInterceptor(HandlerInterceptor interceptor) {
        interceptors.add(interceptor);
        // 重新排序（CopyOnWriteArrayList 需要手动排序）
        List<HandlerInterceptor> sorted = new ArrayList<>(interceptors);
        sorted.sort(Comparator.comparingInt(HandlerInterceptor::getOrder));
        interceptors.clear();
        interceptors.addAll(sorted);
        log.debug("Added interceptor: {} with order {}",
                interceptor.getClass().getSimpleName(), interceptor.getOrder());
    }

    /**
     * 获取操作处理器
     *
     * @param operationType 操作类型
     * @param <O>           操作类型
     * @param <R>           返回类型
     * @return 处理器实例，不存在时返回 null
     */
    @SuppressWarnings("unchecked")
    public <O extends Operation<R>, R> OperationHandler<O, R> getHandler(Class<O> operationType) {
        return (OperationHandler<O, R>) handlers.get(operationType);
    }

    /**
     * Execute an operation
     * <p>
     * 根据操作类型查找处理器，并通过拦截器链执行。
     *
     * @param operation the operation instance
     * @param context   驱动上下文
     * @param <O>       operation type
     * @param <R>       return type
     * @return operation result
     */
    @SuppressWarnings("unchecked")
    public <O extends Operation<R>, R> Mono<R> execute(O operation, DriverContext context) {
        Class<O> operationType = (Class<O>) operation.getClass();
        OperationHandler<O, R> handler = getHandler(operationType);

        if (handler == null) {
            return Mono.error(new UnsupportedOperationException(
                    "No handler registered for operation: " + operationType.getSimpleName()));
        }

        log.debug("Executing operation: {} with handler: {} through {} interceptors",
                operation.getOperationName(), handler.getClass().getSimpleName(), interceptors.size());

        // 构建拦截器链
        InterceptorChain<O, R> chain = buildChain(handler, 0);
        return chain.proceed(operation, context);
    }

    /**
     * 构建拦截器链
     * <p>
     * Recursively builds the interceptor chain from the current index, falling back to handler execution.
     */
    private <O extends Operation<R>, R> InterceptorChain<O, R> buildChain(
            OperationHandler<O, R> handler, int index) {
        if (index >= interceptors.size()) {
            // Chain end: invoke actual handler
            return (op, ctx) -> handler.handle(op, ctx);
        }

        HandlerInterceptor interceptor = interceptors.get(index);
        InterceptorChain<O, R> nextChain = buildChain(handler, index + 1);

        return (op, ctx) -> interceptor.intercept(op, ctx, nextChain);
    }

    /**
     * 检查是否支持某个操作
     *
     * @param operationType 操作类型
     * @return true if supported
     */
    public boolean supports(Class<? extends Operation<?>> operationType) {
        return handlers.containsKey(operationType);
    }

    /**
     * Get the number of registered operations
     *
     * @return 操作数量
     */
    public int size() {
        return handlers.size();
    }

    /**
     * Get the number of registered interceptors
     *
     * @return 拦截器数量
     */
    public int interceptorCount() {
        return interceptors.size();
    }

    /**
     * Get all registered operation types
     *
     * @return 操作类型集合
     */
    public Set<Class<?>> getRegisteredOperationTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    /**
     * Aggregate capabilities declared by all registered handlers
     * <p>
     * Returns the union of getProvidedCapabilities() from all handlers.
     *
     * @return 能力集合
     */
    public Set<Capability> getAggregatedCapabilities() {
        Set<Capability> capabilities = EnumSet.noneOf(Capability.class);
        for (OperationHandler<?, ?> handler : handlers.values()) {
            capabilities.addAll(handler.getProvidedCapabilities());
        }
        return capabilities;
    }
}
