package win.ixuni.chimera.server.auth;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 认证配置
 */
@Data
@ConfigurationProperties(prefix = "chimera.auth")
public class AuthProperties {

    /**
     * Whether authentication is enabled
     */
    private boolean enabled = false;

    /**
     * Static credentials list (for simple scenarios)
     */
    private List<StaticCredential> credentials = new ArrayList<>();

    /**
     * 是否允许匿名读取
     */
    private boolean allowAnonymousRead = false;

    /**
     * 是否允许匿名写入
     */
    private boolean allowAnonymousWrite = false;

    @Data
    public static class StaticCredential {
        private String accessKeyId;
        private String secretAccessKey;
        private String userId;
        private String description;
        private boolean enabled = true;
    }
}
