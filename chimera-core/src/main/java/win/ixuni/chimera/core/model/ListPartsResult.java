package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * List uploaded parts result
 */
@Data
@Builder
public class ListPartsResult {

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
     * 分片列表
     */
    private List<UploadPart> parts;

    /**
     * 是否截断
     */
    private Boolean isTruncated;

    /**
     * Next part number marker
     */
    private Integer nextPartNumberMarker;

    /**
     * 最大分片数
     */
    private Integer maxParts;
}
