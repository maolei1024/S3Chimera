package win.ixuni.chimera.core.operation.interceptor;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.HandlerInterceptor;
import win.ixuni.chimera.core.operation.InterceptorChain;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 日志拦截器
 * <p>
 * 在操作执行前后记录日志，包括执行时间和结果状态。
 */
@Slf4j
public class LoggingInterceptor implements HandlerInterceptor {

    @Override
    public <O extends Operation<R>, R> Mono<R> intercept(
            O operation,
            DriverContext context,
            InterceptorChain<O, R> chain) {

        final long startTime = System.currentTimeMillis();
        final String operationName = operation.getOperationName();
        final String driverName = context.getDriverName();

        log.debug("[{}] Starting operation: {}", driverName, operationName);

        return chain.proceed(operation, context)
                .doOnSuccess(result -> {
                    long duration = System.currentTimeMillis() - startTime;
                    log.debug("[{}] Operation {} completed successfully in {}ms",
                            driverName, operationName, duration);
                })
                .doOnError(error -> {
                    long duration = System.currentTimeMillis() - startTime;
                    log.warn("[{}] Operation {} failed after {}ms: {}",
                            driverName, operationName, duration, error.getMessage());
                });
    }

    @Override
    public int getOrder() {
        return -100; // 最外层拦截器
    }
}
