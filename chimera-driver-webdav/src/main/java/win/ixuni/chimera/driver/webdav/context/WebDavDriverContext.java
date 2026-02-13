package win.ixuni.chimera.driver.webdav.context;

import com.github.sardine.Sardine;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebDAV 驱动上下文
 * <p>
 * Holds the Sardine client and configuration
 */
@Getter
@Builder
public class WebDavDriverContext implements DriverContext {

    private final DriverConfig config;

    /**
     * Sardine WebDAV 客户端
     */
    private final Sardine sardine;

    /**
     * WebDAV 服务器根 URL
     */
    private final String baseUrl;

    /**
     * Multipart upload state: uploadId -> MultipartState
     */
    @Builder.Default
    private final Map<String, MultipartState> multipartUploads = new ConcurrentHashMap<>();

    /**
     * Local temporary directory (for staging multipart parts)
     */
    @Builder.Default
    private final Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), ".chimera-multipart", "webdav");

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
        return "webdav";
    }

    // ============ Path Constants (referencing SidecarMetadata unified definitions) ============

    // ============ URL 工具方法 ============

    /**
     * Get normalized base URL (ensures trailing slash)
     */
    private String getNormalizedBaseUrl() {
        return baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
    }

    /**
     * 获取数据根 URL (s3chimera/data/)
     */
    public String getDataRootUrl() {
        return getNormalizedBaseUrl() + SidecarMetadata.S3_CHIMERA_ROOT + "/" + SidecarMetadata.DATA_DIR + "/";
    }

    /**
     * 获取元数据根 URL (s3chimera/meta/)
     */
    public String getMetaRootUrl() {
        return getNormalizedBaseUrl() + SidecarMetadata.S3_CHIMERA_ROOT + "/" + SidecarMetadata.META_DIR + "/";
    }

    /**
     * 获取 Bucket 的 WebDAV URL（数据目录）
     */
    public String getBucketUrl(String bucketName) {
        return getDataRootUrl() + urlEncodePath(bucketName) + "/";
    }

    /**
     * 获取 Bucket 元数据目录 URL
     */
    public String getBucketMetaUrl(String bucketName) {
        return getMetaRootUrl() + urlEncodePath(bucketName) + "/";
    }

    /**
     * 获取 Object 的 WebDAV URL
     */
    public String getObjectUrl(String bucketName, String key) {
        return getBucketUrl(bucketName) + urlEncodePath(key);
    }

    /**
     * 获取 Object 元数据文件 URL
     */
    public String getMetadataUrl(String bucketName, String key) {
        return getBucketMetaUrl(bucketName) + urlEncodePath(key) + SidecarMetadata.SIDECAR_SUFFIX;
    }

    /**
     * URL 编码路径，保留 / 作为路径分隔符
     */
    private String urlEncodePath(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        String[] segments = path.split("/", -1);
        StringBuilder encoded = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                encoded.append("/");
            }
            encoded.append(URLEncoder.encode(segments[i], StandardCharsets.UTF_8)
                    .replace("+", "%20"));
        }
        return encoded.toString();
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
