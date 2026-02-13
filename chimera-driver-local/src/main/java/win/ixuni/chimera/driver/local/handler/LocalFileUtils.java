package win.ixuni.chimera.driver.local.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import win.ixuni.chimera.core.util.SidecarMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HexFormat;

/**
 * Local 驱动共享工具方法
 */
public final class LocalFileUtils {

    private LocalFileUtils() {
    }

    /**
     * 共享 ObjectMapper 实例（线程安全）
     * <p>
     * Unified config: register JavaTimeModule, disable timestamp format
     */
    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    /**
     * 分片上传临时目录名称
     */
    public static final String MULTIPART_DIR = ".multipart";

    /**
     * 将字节数组转换为十六进制字符串
     */
    public static String bytesToHex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    /**
     * 递归删除目录及其内容
     */
    public static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete: " + path, e);
                    }
                });
    }

    /**
     * 读取 Object 的 Sidecar 元数据（带 fallback）
     * <p>
     * 如果 .s3chimera.meta 文件存在，从 JSON 反序列化；
     * 否则从文件属性构建默认元数据
     */
    public static SidecarMetadata readSidecar(Path objectPath, Path metaPath) throws IOException {
        if (Files.exists(metaPath)) {
            return MAPPER.readValue(Files.readString(metaPath), SidecarMetadata.class);
        }
        // 元数据文件不存在，从文件属性构建
        var attrs = Files.readAttributes(objectPath, java.nio.file.attribute.BasicFileAttributes.class);
        return SidecarMetadata.builder()
                .size(attrs.size())
                .lastModified(attrs.lastModifiedTime().toMillis())
                .contentType("application/octet-stream")
                .build();
    }

    /**
     * 写入 Sidecar 元数据为 JSON
     */
    public static void writeSidecar(Path metaPath, SidecarMetadata sidecar) throws IOException {
        Files.createDirectories(metaPath.getParent());
        Files.writeString(metaPath, MAPPER.writeValueAsString(sidecar));
    }

    /**
     * 删除 Sidecar 元数据文件（如果存在）
     */
    public static void deleteSidecar(Path metaPath) throws IOException {
        Files.deleteIfExists(metaPath);
    }

    /**
     * 判断路径是否在 .multipart 目录下
     */
    public static boolean isMultipartTempFile(Path bucketPath, Path filePath) {
        Path relative = bucketPath.relativize(filePath);
        return relative.toString().replace("\\", "/").startsWith(MULTIPART_DIR + "/");
    }
}
