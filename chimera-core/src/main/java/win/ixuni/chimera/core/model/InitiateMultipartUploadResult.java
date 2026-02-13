package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Create multipart upload response
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "InitiateMultipartUploadResult")
public class InitiateMultipartUploadResult {
    
    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Bucket")
    private String bucket;
    
    /**
     * 对象Key
     */
    @JacksonXmlProperty(localName = "Key")
    private String key;
    
    /**
     * 上传ID
     */
    @JacksonXmlProperty(localName = "UploadId")
    private String uploadId;
}
