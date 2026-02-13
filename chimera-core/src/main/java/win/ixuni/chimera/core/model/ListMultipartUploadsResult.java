package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 列出分片上传会话结果
 */
@Data
@Builder
@JacksonXmlRootElement(localName = "ListMultipartUploadsResult")
public class ListMultipartUploadsResult {

    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Bucket")
    private String bucketName;

    /**
     * Key前缀
     */
    @JacksonXmlProperty(localName = "Prefix")
    private String prefix;

    /**
     * 分隔符
     */
    @JacksonXmlProperty(localName = "Delimiter")
    private String delimiter;

    /**
     * 上传会话列表
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Upload")
    private List<MultipartUpload> uploads;

    /**
     * 是否截断
     */
    @JacksonXmlProperty(localName = "IsTruncated")
    private Boolean isTruncated;

    /**
     * Next key marker
     */
    @JacksonXmlProperty(localName = "NextKeyMarker")
    private String nextKeyMarker;

    /**
     * Next upload ID marker
     */
    @JacksonXmlProperty(localName = "NextUploadIdMarker")
    private String nextUploadIdMarker;

    /**
     * 公共前缀
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "CommonPrefixes")
    private List<String> commonPrefixes;
}
