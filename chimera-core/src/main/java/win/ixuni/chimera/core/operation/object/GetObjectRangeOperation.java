package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 获取对象部分内容操作（Range 请求）
 */
@Value
public class GetObjectRangeOperation implements Operation<S3ObjectData> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;

    /**
     * 起始字节位置（包含）
     */
    long rangeStart;

    /**
     * 结束字节位置（包含），null 表示到文件末尾
     */
    Long rangeEnd;
}
