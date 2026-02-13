package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * SFTP 获取对象范围处理器（Range 请求）
 */
public class SftpGetObjectRangeHandler implements OperationHandler<GetObjectRangeOperation, S3ObjectData> {

    private static final int BUFFER_SIZE = 8192;

    @Override
    public Mono<S3ObjectData> handle(GetObjectRangeOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long rangeStart = operation.getRangeStart();
        Long rangeEnd = operation.getRangeEnd();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String objectPath = ctx.getObjectPath(bucketName, key);

                // 检查文件是否存在
                SftpClient.Attributes attrs;
                try {
                    attrs = sftpClient.stat(objectPath);
                    if (attrs.isDirectory()) {
                        throw new ObjectNotFoundException(bucketName, key);
                    }
                } catch (IOException e) {
                    throw new ObjectNotFoundException(bucketName, key);
                }

                long fileSize = attrs.getSize();
                long effectiveEnd = (rangeEnd == null || rangeEnd >= fileSize) ? fileSize - 1 : rangeEnd;
                int rangeLength = (int) (effectiveEnd - rangeStart + 1);

                // Use SFTP handle-based read at specified position
                byte[] rangeData = new byte[rangeLength];
                try (SftpClient.CloseableHandle handle = sftpClient.open(objectPath, SftpClient.OpenMode.Read)) {
                    int bytesRead = 0;
                    long offset = rangeStart;
                    while (bytesRead < rangeLength) {
                        int toRead = Math.min(BUFFER_SIZE, rangeLength - bytesRead);
                        int read = sftpClient.read(handle, offset, rangeData, bytesRead, toRead);
                        if (read <= 0) {
                            break;
                        }
                        bytesRead += read;
                        offset += read;
                    }
                }

                // Try to read sidecar metadata (consistent with GetObjectHandler)
                String etag = ctx.generateETag(attrs);
                String contentType = "application/octet-stream";
                Map<String, String> userMetadata = null;
                int dotIndex = key.lastIndexOf('.');
                if (dotIndex > 0) {
                    String ext = key.substring(dotIndex + 1).toLowerCase();
                    contentType = switch (ext) {
                        case "txt" -> "text/plain";
                        case "htm", "html" -> "text/html";
                        case "json" -> "application/json";
                        case "xml" -> "application/xml";
                        case "jpg", "jpeg" -> "image/jpeg";
                        case "png" -> "image/png";
                        default -> "application/octet-stream";
                    };
                }

                try {
                    String metaPath = ctx.getMetadataPath(bucketName, key);
                    try (InputStream metaIs = sftpClient.read(metaPath)) {
                        String json = new String(metaIs.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                        SidecarMetadata sidecar = JsonUtils.fromJson(json, SidecarMetadata.class);
                        if (sidecar != null) {
                            if (sidecar.getEtag() != null) {
                                etag = "\"" + sidecar.getEtag() + "\"";
                            }
                            if (sidecar.getContentType() != null) {
                                contentType = sidecar.getContentType();
                            }
                            userMetadata = sidecar.getUserMetadata();
                        }
                    }
                } catch (Exception e) {
                    // Sidecar 不存在或读取失败，回退到普通逻辑
                }

                S3Object metadata = S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size((long) rangeLength)
                        .lastModified(attrs.getModifyTime() != null
                                ? attrs.getModifyTime().toInstant()
                                : null)
                        .etag(etag)
                        .contentType(contentType)
                        .userMetadata(userMetadata)
                        .build();

                return S3ObjectData.builder()
                        .metadata(metadata)
                        .content(Flux.just(ByteBuffer.wrap(rangeData)))
                        .build();
            }
        });
    }

    @Override
    public Class<GetObjectRangeOperation> getOperationType() {
        return GetObjectRangeOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.RANGE_READ);
    }
}
