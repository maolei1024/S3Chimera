package win.ixuni.chimera.core.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Sidecar metadata file structure
 * <p>
 * Used to persist S3 object metadata in filesystem-based drivers (Local/SFTP/WebDAV, etc.).
 * These drivers lack native metadata storage and use sidecar JSON files to persist metadata alongside object data.
 * <p>
 * Unified directory structure:
 * 
 * <pre>
 *   basePath/
 *     s3chimera/
 *       data/          ← 实际对象数据
 *         bucket-name/
 *           key
 *       meta/          ← 元数据（sidecar 文件）
 *         bucket-name/
 *           key.s3chimera.meta
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SidecarMetadata {

    // ============ Shared Path Constants (all filesystem-based drivers) ============

    /**
     * S3Chimera root directory name
     */
    public static final String S3_CHIMERA_ROOT = "s3chimera";

    /**
     * Data subdirectory name
     */
    public static final String DATA_DIR = "data";

    /**
     * Metadata subdirectory name
     */
    public static final String META_DIR = "meta";

    /**
     * Sidecar file suffix
     */
    public static final String SIDECAR_SUFFIX = ".s3chimera.meta";

    // ============ Metadata Fields ============

    /**
     * S3 ETag (MD5)
     */
    private String etag;

    /**
     * Content type
     */
    private String contentType;

    /**
     * User-defined metadata
     */
    private Map<String, String> userMetadata;

    /**
     * Object size
     */
    private Long size;

    /**
     * Last modified time (epoch milliseconds)
     */
    private Long lastModified;

    // ============ Utility Methods ============

    /**
     * Get the sidecar file suffix
     */
    public static String getSidecarSuffix() {
        return SIDECAR_SUFFIX;
    }

    /**
     * Check whether a filename is a sidecar metadata file
     *
     * @param filename the filename (can be a full path or just the filename)
     * @return true if this is a sidecar file
     */
    public static boolean isSidecarFile(String filename) {
        return filename != null && filename.endsWith(SIDECAR_SUFFIX);
    }

    /**
     * Get the last modified time as an Instant
     *
     * @return Instant, or null if lastModified is null
     */
    @JsonIgnore
    public Instant getLastModifiedInstant() {
        return lastModified != null ? Instant.ofEpochMilli(lastModified) : null;
    }
}
