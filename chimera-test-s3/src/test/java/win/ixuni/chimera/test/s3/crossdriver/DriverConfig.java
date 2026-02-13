package win.ixuni.chimera.test.s3.crossdriver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Driver configuration
 * <p>
 * Used to configure each driver in the cross-driver test pool
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DriverConfig {

    /**
     * 驱动名称，如 "driver-mem", "driver-mysql"
     */
    private String name;

    /**
     * 驱动类型，如 "memory", "mysql", "local", "sftp"
     */
    private String type;

    /**
     * Driver-specific configuration
     */
    private Map<String, Object> properties;

    /**
     * Whether enabled
     */
    @Builder.Default
    private boolean enabled = true;

    /**
     * 生成此驱动的测试 bucket 名称
     */
    public String generateBucketName(String suffix) {
        return getBucketPrefix() + suffix;
    }

    public String getBucketPrefix() {
        if (properties != null && properties.containsKey("bucket-prefix")) {
            return properties.get("bucket-prefix").toString();
        }
        return name + "-"; // fallback
    }
}
