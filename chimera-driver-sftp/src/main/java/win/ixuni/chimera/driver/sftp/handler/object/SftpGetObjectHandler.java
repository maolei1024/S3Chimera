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
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

/**
 * SFTP 获取对象处理器
 */
@Slf4j
public class SftpGetObjectHandler implements OperationHandler<GetObjectOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

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

                // 读取文件内容
                byte[] data;
                try (InputStream is = sftpClient.read(objectPath);
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    is.transferTo(baos);
                    data = baos.toByteArray();
                }

                String contentType = "application/octet-stream";
                String etag = ctx.generateETag(attrs); // 默认合成 ETag
                Map<String, String> userMetadata = null;

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
                            userMetadata = sidecar.getUserMetadata();
                        }
                    }
                } catch (Exception e) {
                    // Sidecar 不存在或读取失败，回退到普通逻辑
                    log.debug("Sidecar metadata not found for {}: {}", key, e.getMessage());
                }

                S3Object metadata = S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size(attrs.getSize())
                        .lastModified(attrs.getModifyTime() != null
                                ? attrs.getModifyTime().toInstant()
                                : null)
                        .contentType(contentType)
                        .etag(etag)
                        .userMetadata(userMetadata)
                        .build();

                return S3ObjectData.builder()
                        .metadata(metadata)
                        .content(Flux.just(ByteBuffer.wrap(data)))
                        .build();
            }
        });
    }

    @Override
    public Class<GetObjectOperation> getOperationType() {
        return GetObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
