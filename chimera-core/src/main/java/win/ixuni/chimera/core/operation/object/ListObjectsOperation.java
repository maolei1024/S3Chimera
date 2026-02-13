package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 列出对象操作 (V1)
 */
@Value
public class ListObjectsOperation implements Operation<ListObjectsResult> {

    /**
     * 列出请求参数
     */
    ListObjectsRequest request;
}
