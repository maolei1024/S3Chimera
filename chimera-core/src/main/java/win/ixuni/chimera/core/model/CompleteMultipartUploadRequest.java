package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Data;

import java.util.List;

/**
 * Complete multipart upload request
 * 
 * S3 请求格式：
 * <pre>
 * &lt;CompleteMultipartUpload&gt;
 *     &lt;Part&gt;
 *         &lt;PartNumber&gt;1&lt;/PartNumber&gt;
 *         &lt;ETag&gt;"etag1"&lt;/ETag&gt;
 *     &lt;/Part&gt;
 * &lt;/CompleteMultipartUpload&gt;
 * </pre>
 */
@Data
@JacksonXmlRootElement(localName = "CompleteMultipartUpload")
public class CompleteMultipartUploadRequest {
    
    /**
     * List of uploaded parts
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Part")
    private List<PartInfo> parts;

    /**
     * 分片信息
     */
    @Data
    public static class PartInfo {
        /**
         * 分片编号
         */
        @JacksonXmlProperty(localName = "PartNumber")
        private Integer partNumber;
        
        /**
         * 分片ETag
         */
        @JacksonXmlProperty(localName = "ETag")
        private String etag;
    }
}
