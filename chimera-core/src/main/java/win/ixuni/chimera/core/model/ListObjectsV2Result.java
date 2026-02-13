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
 * ListObjectsV2 返回结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "ListBucketResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListObjectsV2Result {
    
    /**
     * Bucket名称
     */
    @JacksonXmlProperty(localName = "Name")
    private String name;
    
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
     * 最大返回数
     */
    @JacksonXmlProperty(localName = "MaxKeys")
    private Integer maxKeys;
    
    /**
     * 编码类型
     */
    @JacksonXmlProperty(localName = "EncodingType")
    private String encodingType;
    
    /**
     * 返回的对象数量
     */
    @JacksonXmlProperty(localName = "KeyCount")
    private Integer keyCount;
    
    /**
     * Whether truncated (more data available)
     */
    @JacksonXmlProperty(localName = "IsTruncated")
    private Boolean isTruncated;
    
    /**
     * Continuation token (used by current request)
     */
    @JacksonXmlProperty(localName = "ContinuationToken")
    private String continuationToken;
    
    /**
     * Next page continuation token
     */
    @JacksonXmlProperty(localName = "NextContinuationToken")
    private String nextContinuationToken;
    
    /**
     * 起始键
     */
    @JacksonXmlProperty(localName = "StartAfter")
    private String startAfter;
    
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
