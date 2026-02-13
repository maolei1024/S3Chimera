package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 获取对象元数据操作（不含数据）
 */
@Value
public class HeadObjectOperation implements Operation<S3Object> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;
}
