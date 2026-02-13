package win.ixuni.chimera.core.operation;

/**
 * S3 操作基础接口
 * <p>
 * All S3 operations (CreateBucket, PutObject, etc.) implement this interface.
 * 泛型参数 R 表示操作的返回类型。
 * <p>
 * Uses the command pattern; each operation is encapsulated as an independent class for:
 * - 无限扩展新功能
 * - 独立测试每个操作
 * - 驱动可选择性实现支持的操作
 *
 * @param <R> 操作返回类型
 */
public interface Operation<R> {

    /**
     * Get the operation name (for logging and monitoring)
     *
     * @return 操作名称，如 "CreateBucket", "PutObject"
     */
    default String getOperationName() {
        String className = getClass().getSimpleName();
        // Remove "Operation" suffix
        if (className.endsWith("Operation")) {
            return className.substring(0, className.length() - 9);
        }
        return className;
    }
}
