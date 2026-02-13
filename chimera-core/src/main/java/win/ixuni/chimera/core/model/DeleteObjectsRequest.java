package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 批量删除对象请求
 */
@Data
@Builder
public class DeleteObjectsRequest {

    /**
     * Bucket名称
     */
    private String bucketName;

    /**
     * 要删除的对象Key列表
     */
    private List<String> keys;

    /**
     * 是否静默模式（仅返回错误）
     */
    @Builder.Default
    private Boolean quiet = false;
}
