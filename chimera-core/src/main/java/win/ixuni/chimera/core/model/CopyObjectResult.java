package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CopyObject operation response result
 * 
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">CopyObject API</a>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "CopyObjectResult")
public class CopyObjectResult {
    
    /**
     * 对象的 ETag
     */
    @JacksonXmlProperty(localName = "ETag")
    private String etag;
    
    /**
     * 对象的最后修改时间（ISO 8601 格式）
     */
    @JacksonXmlProperty(localName = "LastModified")
    private String lastModified;
}
