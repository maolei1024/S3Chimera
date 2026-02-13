package win.ixuni.chimera.driver.sftp.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;

/**
 * SFTP abort multipart upload handler
 * <p>
 * Clean up local temporary part files
 */
public class SftpAbortMultipartUploadHandler
        implements OperationHandler<AbortMultipartUploadOperation, Void> {

    @Override
    public Mono<Void> handle(AbortMultipartUploadOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String uploadId = operation.getUploadId();

        return Mono.fromRunnable(() -> {
            Path tempPath = ctx.getMultipartTempPath(uploadId);
            try {
                if (Files.exists(tempPath)) {
                    Files.walk(tempPath)
                            .sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.delete(path);
                                } catch (Exception ignored) {
                                }
                            });
                }
            } catch (Exception ignored) {
            }

            ctx.getMultipartUploads().remove(uploadId);
        });
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
