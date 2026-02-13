package win.ixuni.chimera.core.operation.multipart;

import lombok.Value;
import win.ixuni.chimera.core.model.ListMultipartUploadsResult;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 列出正在进行的分片上传操作
 */
@Value
public class ListMultipartUploadsOperation implements Operation<ListMultipartUploadsResult> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * Key 前缀过滤
     */
    String prefix;
}
