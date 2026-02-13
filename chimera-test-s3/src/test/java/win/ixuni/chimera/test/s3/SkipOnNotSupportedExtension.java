package win.ixuni.chimera.test.s3;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * JUnit 5 扩展：自动跳过不支持的功能
 * <p>
 * 当驱动不支持某个功能时，服务器会返回 501 (NotImplemented) 或 500 (含不支持信息)。
 * 此扩展会捕获这类错误，将测试标记为"跳过"而非"失败"。
 * <p>
 * Usage: add @ExtendWith(SkipOnNotSupportedExtension.class) to your test class
 */
public class SkipOnNotSupportedExtension implements TestExecutionExceptionHandler {

    private static final int NOT_IMPLEMENTED_STATUS = 501;
    private static final int INTERNAL_ERROR_STATUS = 500;
    private static final String NOT_IMPLEMENTED_CODE = "NotImplemented";

    // 表示"不支持"的关键词
    private static final String[] UNSUPPORTED_KEYWORDS = {
            "no handler registered",
            "not supported",
            "not implemented",
            "unsupported operation",
            "UnsupportedOperationException"
    };

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (isNotSupportedException(throwable)) {
            String message = extractSkipMessage(throwable);
            throw new TestAbortedException(message, throwable);
        }
        // Other exceptions continue to throw, test fails
        throw throwable;
    }

    /**
     * Determine whether this is an "unsupported" exception
     */
    private boolean isNotSupportedException(Throwable throwable) {
        // 检查 S3Exception
        if (throwable instanceof S3Exception s3ex) {
            // 501 状态码明确表示不支持
            if (s3ex.statusCode() == NOT_IMPLEMENTED_STATUS) {
                return true;
            }
            // 检查 NotImplemented 错误码
            if (s3ex.awsErrorDetails() != null
                    && NOT_IMPLEMENTED_CODE.equals(s3ex.awsErrorDetails().errorCode())) {
                return true;
            }
            // 500 错误但消息包含不支持相关关键词
            if (s3ex.statusCode() == INTERNAL_ERROR_STATUS && containsUnsupportedKeyword(s3ex)) {
                return true;
            }
        }

        // 检查 UnsupportedOperationException
        if (throwable instanceof UnsupportedOperationException) {
            return true;
        }

        // 递归检查 cause
        if (throwable.getCause() != null) {
            return isNotSupportedException(throwable.getCause());
        }

        // 检查 suppressed exceptions
        for (Throwable suppressed : throwable.getSuppressed()) {
            if (isNotSupportedException(suppressed)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if exception message contains unsupported-related keywords
     */
    private boolean containsUnsupportedKeyword(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null) {
            return false;
        }
        String lowerMessage = message.toLowerCase();
        for (String keyword : UNSUPPORTED_KEYWORDS) {
            if (lowerMessage.contains(keyword.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extract skip reason message
     */
    private String extractSkipMessage(Throwable throwable) {
        if (throwable instanceof S3Exception s3ex) {
            String errorMessage = s3ex.awsErrorDetails() != null
                    ? s3ex.awsErrorDetails().errorMessage()
                    : s3ex.getMessage();
            return "Skipped: Driver does not support this feature - " + errorMessage;
        }
        return "Skipped: " + throwable.getMessage();
    }
}
