package win.ixuni.chimera.driver.local.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

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
 * Local 完成分片上传处理器
 */
public class LocalCompleteMultipartUploadHandler
        implements OperationHandler<CompleteMultipartUploadOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CompleteMultipartUploadOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        return Mono.fromCallable(() -> {
            Path objectPath = ctx.getObjectPath(bucketName, key);
            Files.createDirectories(objectPath.getParent());

            MessageDigest md5 = MessageDigest.getInstance("MD5");
            long totalSize = 0;

            // 按 partNumber 排序并合并分片
            try (OutputStream os = Files.newOutputStream(objectPath)) {
                var sortedParts = operation.getParts().stream()
                        .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                        .toList();

                for (CompletedPart part : sortedParts) {
                    Path partPath = ctx.getPartPath(bucketName, uploadId, part.getPartNumber());
                    if (Files.exists(partPath)) {
                        try (InputStream is = Files.newInputStream(partPath)) {
                            byte[] buffer = new byte[8192];
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

            String etag = "\"" + LocalFileUtils.bytesToHex(md5.digest()) + "-" + operation.getParts().size() + "\"";
            Instant now = Instant.now();

            // Write sidecar metadata (consistent with PutObject)
            Path metaPath = ctx.getMetadataPath(bucketName, key);
            SidecarMetadata sidecar = SidecarMetadata.builder()
                    .contentType(state.getContentType())
                    .size(totalSize)
                    .etag(etag)
                    .lastModified(now.toEpochMilli())
                    .userMetadata(state.getMetadata())
                    .build();
            LocalFileUtils.writeSidecar(metaPath, sidecar);

            // 清理临时分片目录
            LocalFileUtils.deleteDirectoryRecursively(ctx.getMultipartPath(bucketName, uploadId));

            // 清理上传状态（内存 + 磁盘）
            ctx.removeMultipartState(uploadId);

            return S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(totalSize)
                    .etag(etag)
                    .lastModified(now)
                    .storageClass("STANDARD")
                    .build();
        });
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
