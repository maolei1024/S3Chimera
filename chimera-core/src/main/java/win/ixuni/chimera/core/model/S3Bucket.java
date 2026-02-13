package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * S3 Bucket 模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class S3Bucket {
    
    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Name")
    private String name;
    
    /**
     * Creation time
     */
    @JacksonXmlProperty(localName = "CreationDate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant creationDate;
    
    /**
     * Owning driver instance name (not serialized to S3 response)
     */
    @JsonIgnore
    private String driverName;
}
