package win.ixuni.chimera.core.operation.bucket;

import lombok.Value;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 删除 Bucket 操作
 */
@Value
public class DeleteBucketOperation implements Operation<Void> {

    /**
     * Bucket 名称
     */
    String bucketName;
}
