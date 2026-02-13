package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

/**
 * 完成分片上传时提交的分片信息
 */
@Data
@Builder
public class CompletedPart {

    /**
     * 分片编号
     */
    private Integer partNumber;

    /**
     * 分片ETag
     */
    private String etag;
}
