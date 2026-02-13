package win.ixuni.chimera.core.operation.bucket;

import lombok.Value;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 检查 Bucket 是否存在操作
 */
@Value
public class BucketExistsOperation implements Operation<Boolean> {

    /**
     * Bucket 名称
     */
    String bucketName;
}
