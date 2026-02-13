package win.ixuni.chimera.core.operation.multipart;

import lombok.Builder;
import lombok.Value;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.Operation;

import java.util.Map;

/**
 * 创建分片上传会话操作
 */
@Value
@Builder
public class CreateMultipartUploadOperation implements Operation<MultipartUpload> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;

    /**
     * Content type
     */
    String contentType;

    /**
     * User metadata
     */
    Map<String, String> metadata;
}
