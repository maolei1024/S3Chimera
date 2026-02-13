package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

/**
 * List uploaded parts request
 */
@Data
@Builder
public class ListPartsRequest {

    /**
     * Bucket名称
     */
    private String bucketName;

    /**
     * 对象Key
     */
    private String key;

    /**
     * 上传ID
     */
    private String uploadId;

    /**
     * 分片编号起始位置
     */
    private Integer partNumberMarker;

    /**
     * 最大返回数量
     */
    @Builder.Default
    private Integer maxParts = 1000;
}
