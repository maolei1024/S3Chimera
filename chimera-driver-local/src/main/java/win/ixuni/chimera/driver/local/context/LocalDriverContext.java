package win.ixuni.chimera.driver.local.context;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地文件系统驱动上下文
 * <p>
 * 提供文件系统操作的共享配置和工具方法
 */
@Slf4j
@Getter
@Builder
public class LocalDriverContext implements DriverContext {

    private final DriverConfig config;

    /**
     * 存储根路径
     */
    private final Path basePath;

    /**
     * Multipart upload state: uploadId -> MultipartState
     */
    @Builder.Default
    private final Map<String, MultipartState> multipartUploads = new ConcurrentHashMap<>();

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
        return "local";
    }

    // ============ Path Constants (referencing SidecarMetadata unified definitions) ============
    private static final String MULTIPART_STATE_FILE = "upload-state.json";

    // ============ Path Utility Methods ============

    /**
     * Get data root path (s3chimera/data)
     */
    public Path getDataRoot() {
        return basePath.resolve(SidecarMetadata.S3_CHIMERA_ROOT).resolve(SidecarMetadata.DATA_DIR);
    }

    /**
     * Get metadata root path (s3chimera/meta)
     */
    public Path getMetaRoot() {
        return basePath.resolve(SidecarMetadata.S3_CHIMERA_ROOT).resolve(SidecarMetadata.META_DIR);
    }

    /**
     * 获取 Bucket 目录路径 (数据目录)
     */
    public Path getBucketPath(String bucketName) {
        return getDataRoot().resolve(bucketName);
    }

    /**
     * Get bucket metadata directory path
     */
    public Path getBucketMetaPath(String bucketName) {
        return getMetaRoot().resolve(bucketName);
    }

    /**
     * 获取 Object 文件路径
     */
    public Path getObjectPath(String bucketName, String key) {
        return getBucketPath(bucketName).resolve(key);
    }

    /**
     * Get object metadata file path
     */
    public Path getMetadataPath(String bucketName, String key) {
        return getBucketMetaPath(bucketName).resolve(key + SidecarMetadata.SIDECAR_SUFFIX);
    }

    /**
     * 获取分片上传临时目录
     */
    public Path getMultipartPath(String bucketName, String uploadId) {
        return getBucketPath(bucketName).resolve(LocalFileUtils.MULTIPART_DIR).resolve(uploadId);
    }

    /**
     * 获取分片文件路径
     */
    public Path getPartPath(String bucketName, String uploadId, int partNumber) {
        return getMultipartPath(bucketName, uploadId).resolve("part-" + partNumber);
    }

    // ============ 分片状态持久化 ============

    /**
     * 获取分片上传状态文件路径
     */
    private Path getMultipartStatePath(String bucketName, String uploadId) {
        return getMultipartPath(bucketName, uploadId).resolve(MULTIPART_STATE_FILE);
    }

    /**
     * 持久化分片上传状态到磁盘
     */
    public void persistMultipartState(MultipartState state) {
        try {
            Path statePath = getMultipartStatePath(state.getBucketName(), state.getUploadId());
            Files.createDirectories(statePath.getParent());
            Files.writeString(statePath, LocalFileUtils.MAPPER.writeValueAsString(state));
        } catch (IOException e) {
            log.warn("Failed to persist multipart state for uploadId: {}", state.getUploadId(), e);
        }
    }

    /**
     * 清理分片上传状态（内存 + 磁盘）
     */
    public void removeMultipartState(String uploadId) {
        var state = multipartUploads.remove(uploadId);
        if (state != null) {
            try {
                Path statePath = getMultipartStatePath(state.getBucketName(), uploadId);
                Files.deleteIfExists(statePath);
            } catch (IOException e) {
                log.warn("Failed to delete multipart state file for uploadId: {}", uploadId, e);
            }
        }
    }

    /**
     * Restore all multipart upload states from disk
     * <p>
     * Iterate all bucket .multipart directories, restoring upload-state.json
     */
    public void restoreMultipartStates() {
        try {
            Path dataRoot = getDataRoot();
            if (!Files.exists(dataRoot)) {
                return;
            }

            Files.list(dataRoot)
                    .filter(Files::isDirectory)
                    .filter(path -> !path.getFileName().toString().startsWith("."))
                    .forEach(bucketDir -> {
                        Path multipartDir = bucketDir.resolve(LocalFileUtils.MULTIPART_DIR);
                        if (!Files.exists(multipartDir)) {
                            return;
                        }
                        try {
                            Files.list(multipartDir)
                                    .filter(Files::isDirectory)
                                    .forEach(uploadDir -> {
                                        Path stateFile = uploadDir.resolve(MULTIPART_STATE_FILE);
                                        if (Files.exists(stateFile)) {
                                            try {
                                                String json = Files.readString(stateFile);
                                                MultipartState state = LocalFileUtils.MAPPER.readValue(
                                                        json, MultipartState.class);
                                                multipartUploads.put(state.getUploadId(), state);
                                                log.info("Restored multipart upload state: {} for key: {}",
                                                        state.getUploadId(), state.getKey());
                                            } catch (IOException e) {
                                                log.warn("Failed to restore multipart state from: {}", stateFile, e);
                                            }
                                        }
                                    });
                        } catch (IOException e) {
                            log.warn("Failed to list multipart directory: {}", multipartDir, e);
                        }
                    });

            log.info("Restored {} multipart upload states", multipartUploads.size());
        } catch (IOException e) {
            log.warn("Failed to restore multipart states", e);
        }
    }

    // ============ Data Structure Definitions ============

    @Getter
    @Builder
    @Jacksonized
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
    @Jacksonized
    public static class PartInfo {
        private final int partNumber;
        private final long size;
        private final String etag;
        private final Instant lastModified;
    }
}
