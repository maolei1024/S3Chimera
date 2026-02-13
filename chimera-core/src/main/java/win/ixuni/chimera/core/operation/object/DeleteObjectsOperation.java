package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 批量删除对象操作
 */
@Value
public class DeleteObjectsOperation implements Operation<DeleteObjectsResult> {

    /**
     * 批量删除请求
     */
    DeleteObjectsRequest request;
}
