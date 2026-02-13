package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Complete multipart upload response
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "CompleteMultipartUploadResult")
public class CompleteMultipartUploadResult {
    
    /**
     * 对象位置URL
     */
    @JacksonXmlProperty(localName = "Location")
    private String location;
    
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
     * 对象ETag
     */
    @JacksonXmlProperty(localName = "ETag")
    private String etag;
}
