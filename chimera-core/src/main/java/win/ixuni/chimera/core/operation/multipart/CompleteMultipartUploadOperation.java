package win.ixuni.chimera.core.operation.multipart;

import lombok.Value;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.Operation;

import java.util.List;

/**
 * Complete multipart upload operation
 */
@Value
public class CompleteMultipartUploadOperation implements Operation<S3Object> {

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

    /**
     * List of uploaded parts
     */
    List<CompletedPart> parts;
}
