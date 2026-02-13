package win.ixuni.chimera.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * CORS 配置
 */
@Data
@ConfigurationProperties(prefix = "chimera.cors")
public class CorsProperties {

    /**
     * Whether CORS is enabled
     */
    private boolean enabled = false;

    /**
     * 允许的源（支持通配符 *）
     */
    private List<String> allowedOrigins = new ArrayList<>(List.of("*"));

    /**
     * 允许的 HTTP 方法
     */
    private List<String> allowedMethods = new ArrayList<>(List.of(
            "GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS"
    ));

    /**
     * 允许的请求头
     */
    private List<String> allowedHeaders = new ArrayList<>(List.of(
            "Authorization",
            "Content-Type",
            "Content-Length",
            "Content-MD5",
            "Cache-Control",
            "X-Amz-*",
            "x-amz-*",
            "Range",
            "If-Match",
            "If-None-Match",
            "If-Modified-Since",
            "If-Unmodified-Since"
    ));

    /**
     * Response headers exposed to client
     */
    private List<String> exposedHeaders = new ArrayList<>(List.of(
            "ETag",
            "Content-Length",
            "Content-Type",
            "Content-Range",
            "Last-Modified",
            "x-amz-request-id",
            "x-amz-version-id",
            "x-amz-delete-marker",
            "Accept-Ranges"
    ));

    /**
     * 是否允许携带凭证
     */
    private boolean allowCredentials = false;

    /**
     * Preflight request cache duration (seconds)
     */
    private long maxAge = 3600;
}
