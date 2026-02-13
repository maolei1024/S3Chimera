package win.ixuni.chimera.driver.sftp.handler.multipart;

import org.apache.sshd.sftp.client.SftpClient;
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
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

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
 * SFTP complete multipart upload handler
 * <p>
 * Merge local parts -> upload to remote SFTP -> return success
 * <p>
 * Important: only returns success after remote upload succeeds, ensuring atomicity
 */
public class SftpCompleteMultipartUploadHandler
        implements OperationHandler<CompleteMultipartUploadOperation, S3Object> {

    private static final int BUFFER_SIZE = 8192;

    @Override
    public Mono<S3Object> handle(CompleteMultipartUploadOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        // Use Schedulers.boundedElastic() for blocking I/O
        return Mono.fromCallable(() -> {
            Path mergedPath = ctx.getMergedTempPath(uploadId);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            long totalSize = 0;

            // 1. Stream-merge parts to local temporary file (without loading to memory)
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

            // 2. Upload merged file to remote SFTP
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String remotePath = ctx.getObjectPath(bucketName, key);

                // Ensure remote parent directory exists
                ensureRemoteParentDirs(sftpClient, remotePath);

                // Stream upload to remote (critical: must succeed before returning success)
                try (InputStream is = Files.newInputStream(mergedPath);
                        OutputStream os = sftpClient.write(remotePath)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        os.write(buffer, 0, bytesRead);
                    }
                }
            }

            // 2.5 Write sidecar metadata file (consistent with PutObjectHandler)
            SidecarMetadata sidecar = SidecarMetadata.builder()
                    .etag(etag.replace("\"", "")) // Preserve multipart suffix
                    .contentType(state.getContentType())
                    .userMetadata(state.getMetadata())
                    .size(totalSize)
                    .lastModified(now.toEpochMilli())
                    .build();
            try (SftpClient sftpClient2 = ctx.createSftpClient()) {
                String metaPath = ctx.getMetadataPath(bucketName, key);
                // Ensure metadata parent directory exists
                int lastSlash = metaPath.lastIndexOf('/');
                if (lastSlash > 0) {
                    ensureRemoteParentDirs(sftpClient2, metaPath);
                }
                try (OutputStream metaOs = sftpClient2.write(metaPath)) {
                    metaOs.write(JsonUtils.toJsonBytes(sidecar));
                }
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
                    // Clean up temporary files even on failure
                    try {
                        cleanupTempDir(ctx.getMultipartTempPath(uploadId));
                        ctx.getMultipartUploads().remove(uploadId);
                    } catch (Exception ignored) {
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private void ensureRemoteParentDirs(SftpClient client, String remotePath) throws Exception {
        int lastSlash = remotePath.lastIndexOf('/');
        if (lastSlash > 0) {
            String parentPath = remotePath.substring(0, lastSlash);
            // Simple check and create parent directory
            try {
                client.stat(parentPath);
            } catch (Exception e) {
                // Recursively create
                ensureRemoteParentDirs(client, parentPath);
                client.mkdir(parentPath);
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
