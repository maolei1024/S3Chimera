package win.ixuni.chimera.driver.webdav.handler.multipart;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.core.util.JsonUtils;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.ByteArrayInputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;

/**
 * WebDAV 完成分片上传处理器
 * <p>
 * 合并本地分片 -> 上传到远程 WebDAV -> 返回成功
 * <p>
 * Important: only returns success after remote upload succeeds, ensuring atomicity
 */
public class WebDavCompleteMultipartUploadHandler
        implements OperationHandler<CompleteMultipartUploadOperation, S3Object> {

    private static final int BUFFER_SIZE = 8192;

    @Override
    public Mono<S3Object> handle(CompleteMultipartUploadOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        return Mono.fromCallable(() -> {
            Path mergedPath = ctx.getMergedTempPath(uploadId);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            long totalSize = 0;

            // 1. 流式合并分片到本地临时文件
            try (OutputStream os = Files.newOutputStream(mergedPath)) {
                var sortedParts = operation.getParts().stream()
                        .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                        .toList();

                for (CompletedPart part : sortedParts) {
                    Path partPath = ctx.getPartTempPath(uploadId, part.getPartNumber());
                    if (Files.exists(partPath)) {
                        try (InputStream is = Files.newInputStream(partPath)) {
                            byte[] buffer = new byte[BUFFER_SIZE];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                os.write(buffer, 0, bytesRead);
                                md5.update(buffer, 0, bytesRead);
                                totalSize += bytesRead;
                            }
                        }
                    }
                }
            }

            String etag = "\"" + bytesToHex(md5.digest()) + "-" + operation.getParts().size() + "\"";
            Instant now = Instant.now();

            // 2. Upload merged file to remote WebDAV (critical: this step must succeed)
            String objectUrl = ctx.getObjectUrl(bucketName, key);

            // 确保父目录存在
            ensureParentDirs(ctx, bucketName, key);

            try (InputStream is = Files.newInputStream(mergedPath)) {
                ctx.getSardine().put(objectUrl, is);
            }

            // 2.5 Write sidecar metadata file (consistent with PutObjectHandler)
            SidecarMetadata sidecar = SidecarMetadata.builder()
                    .etag(etag.replace("\"", ""))
                    .contentType(state.getContentType())
                    .userMetadata(state.getMetadata())
                    .size(totalSize)
                    .lastModified(now.toEpochMilli())
                    .build();
            try {
                String metadataUrl = ctx.getMetadataUrl(bucketName, key);
                // Ensure metadata parent directory exists
                int lastSlash = metadataUrl.lastIndexOf('/');
                if (lastSlash > 0) {
                    String metaParentUrl = metadataUrl.substring(0, lastSlash + 1);
                    ensureDirectoryExists(ctx, metaParentUrl);
                }
                ctx.getSardine().put(metadataUrl,
                        new ByteArrayInputStream(JsonUtils.toJsonBytes(sidecar)),
                        "application/json");
            } catch (Exception ignored) {
                // Sidecar write failure does not affect the main flow
            }

            // 3. Clean up local temporary files
            cleanupTempDir(ctx.getMultipartTempPath(uploadId));
            ctx.getMultipartUploads().remove(uploadId);

            return S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(totalSize)
                    .etag(etag)
                    .lastModified(now)
                    .storageClass("STANDARD")
                    .build();
        })
                .doOnError(e -> {
                    try {
                        cleanupTempDir(ctx.getMultipartTempPath(uploadId));
                        ctx.getMultipartUploads().remove(uploadId);
                    } catch (Exception ignored) {
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private void ensureParentDirs(WebDavDriverContext ctx, String bucketName, String key) throws Exception {
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentKey = key.substring(0, lastSlash);
            String parentUrl = ctx.getObjectUrl(bucketName, parentKey) + "/";
            if (!ctx.getSardine().exists(parentUrl)) {
                // 递归创建父目录
                ensureParentDirs(ctx, bucketName, parentKey);
                ctx.getSardine().createDirectory(parentUrl);
            }
        }
    }

    private void ensureDirectoryExists(WebDavDriverContext ctx, String dirUrl) throws Exception {
        if (!ctx.getSardine().exists(dirUrl)) {
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

    private void cleanupTempDir(Path dir) throws Exception {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Class<CompleteMultipartUploadOperation> getOperationType() {
        return CompleteMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
