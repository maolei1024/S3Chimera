package win.ixuni.chimera.core.operation.object;

import lombok.Value;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.operation.Operation;

/**
 * 列出对象操作 (V2)
 */
@Value
public class ListObjectsV2Operation implements Operation<ListObjectsV2Result> {

    /**
     * 列出请求参数
     */
    ListObjectsRequest request;
}
