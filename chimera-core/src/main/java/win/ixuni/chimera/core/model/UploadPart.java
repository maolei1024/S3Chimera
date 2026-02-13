package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * 分片上传的单个分片
 */
@Data
@Builder
public class UploadPart {

    /**
     * 分片编号 (1-10000)
     */
    private Integer partNumber;

    /**
     * 分片大小（字节）
     */
    private Long size;

    /**
     * ETag
     */
    private String etag;

    /**
     * Last modified time
     */
    private Instant lastModified;
}
