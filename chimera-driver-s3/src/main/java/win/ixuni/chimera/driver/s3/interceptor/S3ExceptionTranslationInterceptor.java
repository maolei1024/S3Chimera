package win.ixuni.chimera.driver.s3.interceptor;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.S3Exception;
import win.ixuni.chimera.core.exception.BucketAlreadyExistsException;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.exception.NoSuchUploadException;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.HandlerInterceptor;
import win.ixuni.chimera.core.operation.InterceptorChain;
import win.ixuni.chimera.core.operation.Operation;

/**
 * S3 exception conversion interceptor
 * <p>
 * Converts AWS SDK S3 exceptions to the Chimera exception hierarchy,
 * Keeps S3 proxy driver exception behavior consistent with other drivers.
 */
@Slf4j
public class S3ExceptionTranslationInterceptor implements HandlerInterceptor {

    @Override
    public <O extends Operation<R>, R> Mono<R> intercept(
            O operation, DriverContext context, InterceptorChain<O, R> chain) {
        return chain.proceed(operation, context)
                .onErrorMap(S3Exception.class, this::translateException);
    }

    /**
     * Convert S3Exception to corresponding Chimera exception
     */
    private Throwable translateException(S3Exception e) {
        String errorCode = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";

        return switch (errorCode) {
            case "NoSuchBucket" -> new BucketNotFoundException(extractBucketName(e));
            case "NoSuchKey" -> new ObjectNotFoundException(extractBucketName(e), extractKey(e));
            case "BucketNotEmpty" -> new BucketNotEmptyException(extractBucketName(e));
            case "BucketAlreadyOwnedByYou", "BucketAlreadyExists" ->
                new BucketAlreadyExistsException(extractBucketName(e));
            case "NoSuchUpload" -> new NoSuchUploadException(extractUploadId(e));
            default -> {
                log.debug("Unmapped S3 error code: {} (HTTP {}), passing through", errorCode, e.statusCode());
                yield e;
            }
        };
    }

    private String extractBucketName(S3Exception e) {
        // S3Exception 的 message 中通常包含资源信息，这里做 fallback
        return e.getMessage() != null ? e.getMessage() : "unknown";
    }

    private String extractKey(S3Exception e) {
        return e.getMessage() != null ? e.getMessage() : "unknown";
    }

    private String extractUploadId(S3Exception e) {
        return e.getMessage() != null ? e.getMessage() : "unknown";
    }

    @Override
    public int getOrder() {
        // Execute at chain end (exception conversion should be handled last)
        return 100;
    }
}
