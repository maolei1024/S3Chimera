package win.ixuni.chimera.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * S3Chimera main configuration
 */
@Data
@ConfigurationProperties(prefix = "chimera")
public class ChimeraProperties {

    /**
     * List of driver configurations
     */
    private List<DriverConfig> drivers = new ArrayList<>();

    /**
     * Routing configuration
     */
    private RoutingConfig routing = new RoutingConfig();

    /**
     * Server configuration
     */
    private ServerConfig server = new ServerConfig();

    /**
     * Routing configuration
     */
    @Data
    public static class RoutingConfig {
        
        /**
         * Default driver instance name
         * <p>
         * Used when no routing rules match
         */
        private String defaultDriver;

        /**
         * Bucket routing rules
         */
        private List<BucketRoutingRule> buckets = new ArrayList<>();
    }

    /**
     * Server configuration
     */
    @Data
    public static class ServerConfig {
        
        /**
         * S3 API path prefix
         */
        private String pathPrefix = "";

        /**
         * Whether to enable virtual-host-style (bucket.endpoint.com)
         */
        private boolean virtualHostStyle = false;

        /**
         * Base domain for virtual-host-style
         * <p>
         * e.g. if set to "s3.example.com", then "mybucket.s3.example.com" resolves to bucket "mybucket"
         * <p>
         * If not set, the first subdomain is extracted from the Host header as the bucket name
         */
        private String baseDomain;

        /**
         * Region name
         */
        private String region = "us-east-1";
    }
}
