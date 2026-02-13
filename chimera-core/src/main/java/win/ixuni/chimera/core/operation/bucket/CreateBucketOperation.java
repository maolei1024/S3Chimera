package win.ixuni.chimera.core.operation.bucket;

import lombok.Value;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 创建 Bucket 操作
 */
@Value
public class CreateBucketOperation implements Operation<S3Bucket> {

    /**
     * Bucket 名称
     */
    String bucketName;
}
