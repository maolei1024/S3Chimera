package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

/**
 * 列出对象请求参数
 */
@Data
@Builder
public class ListObjectsRequest {
    
    /**
     * Bucket名称
     */
    private String bucketName;
    
    /**
     * 前缀过滤
     */
    private String prefix;
    
    /**
     * Delimiter (for simulating directory structure)
     */
    @Builder.Default
    private String delimiter = "/";
    
    /**
     * Start position (for pagination, V1)
     */
    private String marker;
    
    /**
     * 最大返回数量
     */
    @Builder.Default
    private Integer maxKeys = 1000;
    
    // ==================== V2-specific Parameters ====================
    
    /**
     * Continuation token (used by V2)
     */
    private String continuationToken;
    
    /**
     * Start-after key (V2, list objects after this key)
     */
    private String startAfter;
    
    /**
     * 是否获取Owner信息
     */
    @Builder.Default
    private Boolean fetchOwner = false;
    
    /**
     * Whether to use V2 API
     */
    @Builder.Default
    private Boolean useV2 = false;
}
