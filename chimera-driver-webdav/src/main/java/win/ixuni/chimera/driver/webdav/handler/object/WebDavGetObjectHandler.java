package win.ixuni.chimera.driver.webdav.handler.object;

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
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * WebDAV 获取对象处理器
 */
public class WebDavGetObjectHandler implements OperationHandler<GetObjectOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            String objectUrl = ctx.getObjectUrl(bucketName, key);

            if (!ctx.getSardine().exists(objectUrl)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            // 获取元数据
            var resources = ctx.getSardine().list(objectUrl, 0);
            var resource = resources.isEmpty() ? null : resources.get(0);

            // 下载内容
            byte[] data;
            try (var is = ctx.getSardine().get(objectUrl);
                    var baos = new ByteArrayOutputStream()) {
                is.transferTo(baos);
                data = baos.toByteArray();
            }

            String etag = resource != null ? resource.getEtag() : null;
            String contentType = resource != null ? resource.getContentType() : "application/octet-stream";
            Map<String, String> userMetadata = null;

            // 尝试读取 Sidecar 元数据（从隔离的 meta 目录）
            String metadataUrl = ctx.getMetadataUrl(bucketName, key);
            try {
                if (ctx.getSardine().exists(metadataUrl)) {
                    try (InputStream sidecarIs = ctx.getSardine().get(metadataUrl)) {
                        byte[] bytes = sidecarIs.readAllBytes();
                        String json = new String(bytes, StandardCharsets.UTF_8);
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
                }
            } catch (Exception e) {
                // 忽略 Sidecar 读取失败
            }

            S3Object metadata = S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size((long) data.length)
                    .etag(etag)
                    .lastModified(
                            resource != null && resource.getModified() != null ? resource.getModified().toInstant()
                                    : null)
                    .contentType(contentType)
                    .userMetadata(userMetadata)
                    .build();

            return S3ObjectData.builder()
                    .metadata(metadata)
                    .content(Flux.just(ByteBuffer.wrap(data)))
                    .build();
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
