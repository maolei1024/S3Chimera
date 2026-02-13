package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 删除对象操作
 */
@Value
public class DeleteObjectOperation implements Operation<Void> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;
}
