package win.ixuni.chimera.driver.sftp.context;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SFTP driver context
 * <p>
 * Holds the SFTP client and configuration
 */
@Getter
@Builder
public class SftpDriverContext implements DriverContext {

    private final DriverConfig config;

    /**
     * SSH client session
     */
    private final ClientSession session;

    /**
     * Create a new SFTP client instance
     * <p>
     * Note: must be closed after use to release the SSH channel
     */
    public SftpClient createSftpClient() throws java.io.IOException {
        return SftpClientFactory.instance().createSftpClient(session);
    }

    /**
     * Base path on the SFTP server
     */
    private final String basePath;

    /**
     * Multipart upload state: uploadId -> MultipartState
     */
    @Builder.Default
    private final Map<String, MultipartState> multipartUploads = new ConcurrentHashMap<>();

    /**
     * Local temporary directory (for staging multipart parts)
     */
    @Builder.Default
    private final Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), ".chimera-multipart", "sftp");

    /**
     * Operation handler registry (injected at runtime)
     */
    @Setter
    private OperationHandlerRegistry handlerRegistry;

    @Override
    public DriverConfig getConfig() {
        return config;
    }

    @Override
    public String getDriverName() {
        return config.getName();
    }

    @Override
    public String getDriverType() {
        return "sftp";
    }

    // ============ ETag Utility Methods ============

    /**
     * Generate ETag from file attributes
     * <p>
     * Uses hex string in "Size-LastModifiedMillis" format
     */
    public String generateETag(SftpClient.Attributes attrs) {
        long size = attrs.getSize();
        long mtime = attrs.getModifyTime() != null ? attrs.getModifyTime().toMillis() : 0;
        return String.format("\"%x-%x\"", size, mtime);
    }

    // ============ Path Constants (referencing SidecarMetadata unified definitions) ============

    // ============ Path Utility Methods ============

    /**
     * Get normalized base path (ensures no trailing slash)
     */
    private String getNormalizedBasePath() {
        return basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    }

    /**
     * Get data root path (s3chimera/data)
     */
    public String getDataRoot() {
        return getNormalizedBasePath() + "/" + SidecarMetadata.S3_CHIMERA_ROOT + "/" + SidecarMetadata.DATA_DIR;
    }

    /**
     * Get metadata root path (s3chimera/meta)
     */
    public String getMetaRoot() {
        return getNormalizedBasePath() + "/" + SidecarMetadata.S3_CHIMERA_ROOT + "/" + SidecarMetadata.META_DIR;
    }

    /**
     * Get bucket directory path (remote SFTP data directory)
     */
    public String getBucketPath(String bucketName) {
        return getDataRoot() + "/" + bucketName;
    }

    /**
     * Get bucket metadata directory path
     */
    public String getBucketMetaPath(String bucketName) {
        return getMetaRoot() + "/" + bucketName;
    }

    /**
     * Get object file path (remote SFTP)
     */
    public String getObjectPath(String bucketName, String key) {
        return getBucketPath(bucketName) + "/" + key;
    }

    /**
     * Get object metadata file path
     */
    public String getMetadataPath(String bucketName, String key) {
        return getBucketMetaPath(bucketName) + "/" + key + SidecarMetadata.SIDECAR_SUFFIX;
    }

    // ============ Local Temporary Part Paths ============

    /**
     * Get multipart upload local temporary directory
     */
    public Path getMultipartTempPath(String uploadId) {
        return tempDir.resolve(uploadId);
    }

    /**
     * Get part file local path
     */
    public Path getPartTempPath(String uploadId, int partNumber) {
        return getMultipartTempPath(uploadId).resolve("part-" + partNumber);
    }

    /**
     * Get merged temporary file path
     */
    public Path getMergedTempPath(String uploadId) {
        return getMultipartTempPath(uploadId).resolve("merged");
    }

    // ============ Data Structure Definitions ============

    @Getter
    @Builder
    public static class MultipartState {
        private final String uploadId;
        private final String bucketName;
        private final String key;
        private final String contentType;
        private final Map<String, String> metadata;
        private final Instant initiated;
        @Builder.Default
        private final Map<Integer, PartInfo> parts = new ConcurrentHashMap<>();
    }

    @Getter
    @Builder
    public static class PartInfo {
        private final int partNumber;
        private final long size;
        private final String etag;
        private final Instant lastModified;
    }
}
