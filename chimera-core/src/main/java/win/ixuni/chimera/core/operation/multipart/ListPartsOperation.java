package win.ixuni.chimera.core.operation.multipart;

import lombok.Value;
import win.ixuni.chimera.core.model.ListPartsRequest;
import win.ixuni.chimera.core.model.ListPartsResult;
import win.ixuni.chimera.core.operation.Operation;

/**
 * List uploaded parts operation
 */
@Value
public class ListPartsOperation implements Operation<ListPartsResult> {

    /**
     * 请求参数
     */
    ListPartsRequest request;
}
