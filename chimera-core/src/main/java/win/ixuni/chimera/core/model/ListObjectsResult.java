package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 列出对象返回结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "ListBucketResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListObjectsResult {
    
    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Name")
    private String bucketName;
    
    /**
     * 前缀
     */
    @JacksonXmlProperty(localName = "Prefix")
    private String prefix;
    
    /**
     * 分隔符
     */
    @JacksonXmlProperty(localName = "Delimiter")
    private String delimiter;
    
    /**
     * Whether truncated (more data available)
     */
    @JacksonXmlProperty(localName = "IsTruncated")
    private Boolean isTruncated;
    
    /**
     * Next page marker
     */
    @JacksonXmlProperty(localName = "NextMarker")
    private String nextMarker;
    
    /**
     * 对象列表
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Contents")
    private List<S3Object> contents;
    
    /**
     * 公共前缀列表（模拟目录）
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "CommonPrefixes")
    private List<CommonPrefix> commonPrefixes;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CommonPrefix {
        @JacksonXmlProperty(localName = "Prefix")
        private String prefix;
    }
}
