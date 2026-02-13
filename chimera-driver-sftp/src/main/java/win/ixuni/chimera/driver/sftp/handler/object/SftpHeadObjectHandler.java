package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * SFTP 获取对象元数据处理器
 */
@Slf4j
public class SftpHeadObjectHandler implements OperationHandler<HeadObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(HeadObjectOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String objectPath = ctx.getObjectPath(bucketName, key);

                SftpClient.Attributes attrs;
                try {
                    attrs = sftpClient.stat(objectPath);
                    if (attrs.isDirectory()) {
                        throw new ObjectNotFoundException(bucketName, key);
                    }
                } catch (IOException e) {
                    throw new ObjectNotFoundException(bucketName, key);
                }

                String contentType = "application/octet-stream";
                String etag = ctx.generateETag(attrs); // 默认合成 ETag

                // 尝试读取 Sidecar 元数据（从隔离的 meta 目录）
                String metadataPath = ctx.getMetadataPath(bucketName, key);
                try {
                    try (InputStream is = sftpClient.read(metadataPath)) {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] buffer = new byte[4096];
                        int len;
                        while ((len = is.read(buffer)) != -1) {
                            baos.write(buffer, 0, len);
                        }
                        String json = baos.toString(StandardCharsets.UTF_8);
                        SidecarMetadata sidecar = JsonUtils.fromJson(json, SidecarMetadata.class);
                        if (sidecar != null) {
                            if (sidecar.getEtag() != null) {
                                etag = "\"" + sidecar.getEtag() + "\"";
                            }
                            if (sidecar.getContentType() != null) {
                                contentType = sidecar.getContentType();
                            }
                            return S3Object.builder()
                                    .bucketName(bucketName)
                                    .key(key)
                                    .size(attrs.getSize())
                                    .lastModified(attrs.getModifyTime() != null
                                            ? attrs.getModifyTime().toInstant()
                                            : null)
                                    .etag(etag)
                                    .contentType(contentType)
                                    .userMetadata(sidecar.getUserMetadata())
                                    .build();
                        }
                    }
                } catch (Exception e) {
                    // 如果 Sidecar 不存在或读取失败，回退到普通逻辑
                    log.debug("Sidecar metadata not found or invalid for {}: {}", key, e.getMessage());
                }

                // 普通逻辑：根据后缀猜测 ContentType
                int dotIndex = key.lastIndexOf('.');
                if (dotIndex > 0) {
                    String ext = key.substring(dotIndex + 1).toLowerCase();
                    if ("txt".equals(ext)) {
                        contentType = "text/plain";
                    } else if ("htm".equals(ext) || "html".equals(ext)) {
                        contentType = "text/html";
                    } else if ("json".equals(ext)) {
                        contentType = "application/json";
                    } else if ("xml".equals(ext)) {
                        contentType = "application/xml";
                    } else if ("jpg".equals(ext) || "jpeg".equals(ext)) {
                        contentType = "image/jpeg";
                    } else if ("png".equals(ext)) {
                        contentType = "image/png";
                    }
                }

                return S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size(attrs.getSize())
                        .lastModified(attrs.getModifyTime() != null
                                ? attrs.getModifyTime().toInstant()
                                : null)
                        .etag(etag)
                        .contentType(contentType)
                        .build();
            }
        });
    }

    @Override
    public Class<HeadObjectOperation> getOperationType() {
        return HeadObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
