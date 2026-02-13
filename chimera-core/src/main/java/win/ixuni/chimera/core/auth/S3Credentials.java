package win.ixuni.chimera.core.auth;

import lombok.Builder;
import lombok.Data;

/**
 * S3 credentials
 */
@Data
@Builder
public class S3Credentials {

    /**
     * Access Key ID
     */
    private String accessKeyId;

    /**
     * Secret Access Key
     */
    private String secretAccessKey;

    /**
     * Whether enabled
     */
    @Builder.Default
    private boolean enabled = true;

    /**
     * Associated user/tenant ID
     */
    private String userId;

    /**
     * Description
     */
    private String description;
}
