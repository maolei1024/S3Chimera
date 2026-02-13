package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 复制对象操作
 */
@Value
public class CopyObjectOperation implements Operation<S3Object> {

    /**
     * 源 Bucket 名称
     */
    String sourceBucket;

    /**
     * 源对象 Key
     */
    String sourceKey;

    /**
     * 目标 Bucket 名称
     */
    String destinationBucket;

    /**
     * 目标对象 Key
     */
    String destinationKey;
}
