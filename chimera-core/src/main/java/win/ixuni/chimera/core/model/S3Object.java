package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * S3 Object 模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class S3Object {
    
    /**
     * 对象Key
     */
    @JacksonXmlProperty(localName = "Key")
    private String key;
    
    /**
     * Owning bucket (not serialized to S3 response, internal use)
     */
    @JsonIgnore
    private String bucketName;
    
    /**
     * 对象大小(字节)
     */
    @JacksonXmlProperty(localName = "Size")
    private Long size;
    
    /**
     * ETag (通常是MD5)
     */
    @JacksonXmlProperty(localName = "ETag")
    private String etag;
    
    /**
     * Last modified time
     */
    @JacksonXmlProperty(localName = "LastModified")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant lastModified;
    
    /**
     * Content type (not serialized in list responses)
     */
    @JsonIgnore
    private String contentType;
    
    /**
     * User-defined metadata (not serialized in list responses)
     */
    @JsonIgnore
    private Map<String, String> userMetadata;
    
    /**
     * 存储类型
     */
    @JacksonXmlProperty(localName = "StorageClass")
    @Builder.Default
    private String storageClass = "STANDARD";
    
    /**
     * Owner信息
     */
    @JacksonXmlProperty(localName = "Owner")
    private Owner owner;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Owner {
        @JacksonXmlProperty(localName = "ID")
        private String id;

        @JacksonXmlProperty(localName = "DisplayName")
        private String displayName;
    }
}
