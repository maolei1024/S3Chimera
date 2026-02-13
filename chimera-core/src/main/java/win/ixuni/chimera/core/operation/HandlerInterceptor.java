package win.ixuni.chimera.core.operation;

import reactor.core.publisher.Mono;

/**
 * Handler 拦截器接口
 * <p>
 * Allows inserting common logic before and after handler execution, such as logging, metering, throttling, etc.
 * Designed using the chain-of-responsibility pattern.
 */
public interface HandlerInterceptor {

    /**
     * 拦截 Handler 执行
     * <p>
     * Implementors can execute custom logic before and after calling chain.proceed().
     *
     * @param operation the operation instance
     * @param context   驱动上下文
     * @param chain     后续拦截器链
     * @param <O>       operation type
     * @param <R>       return type
     * @return operation result
     */
    <O extends Operation<R>, R> Mono<R> intercept(
            O operation,
            DriverContext context,
            InterceptorChain<O, R> chain);

    /**
     * Get interceptor priority (lower number = higher priority)
     * <p>
     * Default priority is 0. Negative values execute earlier, positive values execute later.
     *
     * @return priority ordinal
     */
    default int getOrder() {
        return 0;
    }
}
