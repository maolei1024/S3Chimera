package win.ixuni.chimera.driver.memory.context;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import win.ixuni.chimera.core.config.DriverConfig;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandlerRegistry;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Memory 驱动上下文
 * <p>
 * 提供内存存储的共享数据结构
 */
@Getter
@Builder
public class MemoryDriverContext implements DriverContext {

    private final DriverConfig config;

    /**
     * Bucket 存储：bucketName -> BucketInfo
     */
    @Builder.Default
    private final Map<String, BucketInfo> buckets = new ConcurrentHashMap<>();

    /**
     * 对象存储：bucketName/key -> ObjectData
     */
    @Builder.Default
    private final Map<String, ObjectData> objects = new ConcurrentHashMap<>();

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
        return "memory";
    }

    // ============ Data Structure Definitions ============

    @Getter
    @Builder
    public static class BucketInfo {
        private final String name;
        private final Instant creationDate;
    }

    @Getter
    @Builder
    public static class ObjectData {
        private final String bucketName;
        private final String key;
        private final byte[] data;
        private final String etag;
        private final String contentType;
        private final Instant lastModified;
        private final Map<String, String> metadata;
    }

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
        private final Map<Integer, PartData> parts = new ConcurrentHashMap<>();
    }

    @Getter
    @Builder
    public static class PartData {
        private final byte[] data;
        private final String etag;
        private final Instant lastModified;
    }

    // ============ Utility Methods ============

    public String objectKey(String bucketName, String key) {
        return bucketName + "/" + key;
    }
}
