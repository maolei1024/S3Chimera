package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * 分片上传会话
 */
@Data
@Builder
public class MultipartUpload {

    /**
     * Upload ID (unique identifier)
     */
    @JacksonXmlProperty(localName = "UploadId")
    private String uploadId;

    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Bucket")
    private String bucketName;

    /**
     * 对象Key
     */
    @JacksonXmlProperty(localName = "Key")
    private String key;

    /**
     * 发起时间
     */
    @JacksonXmlProperty(localName = "Initiated")
    private Instant initiated;

    /**
     * Content type
     */
    @JacksonXmlProperty(localName = "ContentType")
    private String contentType;

    /**
     * 存储类型
     */
    @JacksonXmlProperty(localName = "StorageClass")
    @Builder.Default
    private String storageClass = "STANDARD";
}
