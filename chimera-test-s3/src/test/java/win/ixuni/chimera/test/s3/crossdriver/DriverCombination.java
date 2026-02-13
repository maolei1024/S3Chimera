package win.ixuni.chimera.test.s3.crossdriver;

import lombok.Builder;
import lombok.Data;

/**
 * 驱动组合
 * <p>
 * Represents a cross-driver test scenario: Source Driver -> Target Driver
 */
@Data
@Builder
public class DriverCombination {

    /**
     * 源驱动配置
     */
    private DriverConfig source;

    /**
     * 目标驱动配置
     */
    private DriverConfig target;

    /**
     * Get combination name for test reports
     */
    public String getName() {
        return source.getName() + " -> " + target.getName();
    }

    /**
     * 获取源 bucket 名称
     */
    public String getSourceBucket(String testId) {
        return source.generateBucketName("src-" + testId);
    }

    /**
     * 获取目标 bucket 名称
     */
    public String getTargetBucket(String testId) {
        return target.generateBucketName("dst-" + testId);
    }

    /**
     * 判断是否为跨驱动组合（源和目标不同类型）
     */
    public boolean isCrossDriver() {
        return !source.getType().equals(target.getType());
    }

    @Override
    public String toString() {
        return getName();
    }
}
