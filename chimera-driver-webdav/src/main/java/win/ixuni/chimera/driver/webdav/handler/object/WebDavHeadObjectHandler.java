package win.ixuni.chimera.driver.webdav.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Set;

import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * WebDAV 获取对象元数据处理器
 */
public class WebDavHeadObjectHandler implements OperationHandler<HeadObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(HeadObjectOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            String objectUrl = ctx.getObjectUrl(bucketName, key);

            if (!ctx.getSardine().exists(objectUrl)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            var resources = ctx.getSardine().list(objectUrl, 0);
            if (resources.isEmpty()) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            var resource = resources.get(0);

            String etag = resource.getEtag();
            String contentType = resource.getContentType();
            java.util.Map<String, String> userMetadata = null;

            // 尝试读取 Sidecar 元数据（从隔离的 meta 目录）
            String metadataUrl = ctx.getMetadataUrl(bucketName, key);
            try {
                if (ctx.getSardine().exists(metadataUrl)) {
                    try (InputStream is = ctx.getSardine().get(metadataUrl)) {
                        byte[] bytes = is.readAllBytes();
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

            return S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(resource.getContentLength())
                    .etag(etag)
                    .lastModified(resource.getModified() != null ? resource.getModified().toInstant() : null)
                    .contentType(contentType)
                    .userMetadata(userMetadata)
                    .build();
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
