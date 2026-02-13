package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 获取对象操作（包含数据流）
 */
@Value
public class GetObjectOperation implements Operation<S3ObjectData> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;
}
