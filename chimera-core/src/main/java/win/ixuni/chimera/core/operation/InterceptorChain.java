package win.ixuni.chimera.core.operation;

import reactor.core.publisher.Mono;

/**
 * Handler 拦截器链接口
 * <p>
 * Used in interceptors to invoke the next interceptor or the final handler.
 *
 * @param <O> 操作类型
 * @param <R> 返回类型
 */
public interface InterceptorChain<O extends Operation<R>, R> {

    /**
     * 继续执行拦截器链
     *
     * @param operation the operation instance
     * @param context   驱动上下文
     * @return operation result
     */
    Mono<R> proceed(O operation, DriverContext context);
}
