package win.ixuni.chimera.driver.webdav.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * WebDAV 获取对象范围处理器（Range 请求）
 */
public class WebDavGetObjectRangeHandler implements OperationHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectRangeOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long rangeStart = operation.getRangeStart();
        Long rangeEnd = operation.getRangeEnd();

        return Mono.fromCallable(() -> {
            String objectUrl = ctx.getObjectUrl(bucketName, key);

            if (!ctx.getSardine().exists(objectUrl)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            // 获取对象元数据
            var resources = ctx.getSardine().list(objectUrl, 0);
            var resource = resources.isEmpty() ? null : resources.get(0);
            long fileSize = resource != null ? resource.getContentLength() : 0;
            long effectiveEnd = (rangeEnd == null || rangeEnd >= fileSize) ? fileSize - 1 : rangeEnd;

            // 构建 Range 头
            Map<String, String> headers = new HashMap<>();
            String rangeHeader = "bytes=" + rangeStart + "-" + effectiveEnd;
            headers.put("Range", rangeHeader);

            // Use get method with headers to send Range request
            byte[] rangeData;
            try (var is = ctx.getSardine().get(objectUrl, headers);
                    var baos = new ByteArrayOutputStream()) {
                is.transferTo(baos);
                rangeData = baos.toByteArray();
            }

            String etag = resource != null ? resource.getEtag() : null;
            String contentType = resource != null ? resource.getContentType() : "application/octet-stream";
            Map<String, String> userMetadata = null;

            // Try to read sidecar metadata (consistent with GetObjectHandler)
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
                    .size((long) rangeData.length)
                    .etag(etag)
                    .lastModified(resource != null && resource.getModified() != null
                            ? resource.getModified().toInstant()
                            : null)
                    .contentType(contentType)
                    .userMetadata(userMetadata)
                    .build();

            return S3ObjectData.builder()
                    .metadata(metadata)
                    .content(Flux.just(ByteBuffer.wrap(rangeData)))
                    .build();
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
