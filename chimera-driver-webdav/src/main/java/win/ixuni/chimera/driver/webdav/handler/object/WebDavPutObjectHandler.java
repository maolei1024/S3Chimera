package win.ixuni.chimera.driver.webdav.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;

import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * WebDAV 上传对象处理器
 */
public class WebDavPutObjectHandler implements OperationHandler<PutObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(PutObjectOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return operation.getContent()
                .reduce(new ByteArrayOutputStream(), (baos, buffer) -> {
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    baos.write(bytes, 0, bytes.length);
                    return baos;
                })
                .flatMap(baos -> Mono.fromCallable(() -> {
                    // 先检查 bucket 是否存在
                    String bucketUrl = ctx.getBucketUrl(bucketName);
                    if (!ctx.getSardine().exists(bucketUrl)) {
                        throw new BucketNotFoundException(bucketName);
                    }

                    byte[] data = baos.toByteArray();
                    String objectUrl = ctx.getObjectUrl(bucketName, key);

                    // 确保父目录存在（数据目录）
                    ensureParentDirectories(ctx, ctx.getBucketUrl(bucketName), key);

                    // Ensure metadata parent directory exists
                    String metadataUrl = ctx.getMetadataUrl(bucketName, key);
                    ensureParentDirectoriesForUrl(ctx, metadataUrl);

                    // 上传文件 (PUT)
                    ctx.getSardine().put(objectUrl, new ByteArrayInputStream(data),
                            operation.getContentType());

                    // 计算 MD5 作为 ETag
                    String etag;
                    try {
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        byte[] digest = md.digest(data);
                        etag = HexFormat.of().formatHex(digest);
                    } catch (Exception e) {
                        etag = UUID.randomUUID().toString().replace("-", ""); // Fallback
                    }

                    // 写入 Sidecar 元数据文件（隔离在 meta 目录）
                    SidecarMetadata sidecar = SidecarMetadata.builder()
                            .etag(etag)
                            .contentType(operation.getContentType())
                            .userMetadata(operation.getMetadata())
                            .size((long) data.length)
                            .lastModified(System.currentTimeMillis())
                            .build();

                    try {
                        ctx.getSardine().put(metadataUrl, JsonUtils.toJsonBytes(sidecar), "application/json");
                    } catch (Exception e) {
                        // 忽略 Sidecar 写入失败
                    }

                    return S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size((long) data.length)
                            .etag("\"" + etag + "\"")
                            .lastModified(Instant.now())
                            .contentType(operation.getContentType())
                            .userMetadata(operation.getMetadata())
                            .build();
                }));
    }

    private void ensureParentDirectories(WebDavDriverContext ctx, String baseUrl, String key) throws Exception {
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentPath = key.substring(0, lastSlash);
            String[] parts = parentPath.split("/");
            StringBuilder path = new StringBuilder();

            for (String part : parts) {
                path.append(part).append("/");
                String dirUrl = baseUrl + path;
                if (!ctx.getSardine().exists(dirUrl)) {
                    ctx.getSardine().createDirectory(dirUrl);
                }
            }
        }
    }

    /**
     * 确保给定 URL 的父目录存在
     */
    private void ensureParentDirectoriesForUrl(WebDavDriverContext ctx, String url) throws Exception {
        int lastSlash = url.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentUrl = url.substring(0, lastSlash + 1);
            ensureDirectoryExists(ctx, parentUrl);
        }
    }

    private void ensureDirectoryExists(WebDavDriverContext ctx, String dirUrl) throws Exception {
        if (!ctx.getSardine().exists(dirUrl)) {
            // 父目录可能也不存在，先递归创建
            int lastSlash = dirUrl.substring(0, dirUrl.length() - 1).lastIndexOf('/');
            if (lastSlash > 0) {
                ensureDirectoryExists(ctx, dirUrl.substring(0, lastSlash + 1));
            }
            try {
                ctx.getSardine().createDirectory(dirUrl);
            } catch (Exception ignored) {
                // May have been concurrently created
            }
        }
    }

    @Override
    public Class<PutObjectOperation> getOperationType() {
        return PutObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
