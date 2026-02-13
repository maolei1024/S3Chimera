package win.ixuni.chimera.core.operation.multipart;

import lombok.Value;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 取消分片上传操作
 */
@Value
public class AbortMultipartUploadOperation implements Operation<Void> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;

    /**
     * 上传 ID
     */
    String uploadId;
}
