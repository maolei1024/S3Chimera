package win.ixuni.chimera.driver.sftp.handler.multipart;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SFTP upload part handler
 * <p>
 * Stream-write part data to local temporary file (memory-safe)
 */
public class SftpUploadPartHandler implements OperationHandler<UploadPartOperation, UploadPart> {

    @Override
    public Mono<UploadPart> handle(UploadPartOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        Path partPath = ctx.getPartTempPath(uploadId, partNumber);
        AtomicLong totalSize = new AtomicLong(0);

        // Stream-write to local temporary file (without loading to memory)
        return Mono.fromCallable(() -> {
            Files.createDirectories(partPath.getParent());
            return MessageDigest.getInstance("MD5");
        })
                .flatMap(md5 -> {
                    try {
                        OutputStream os = Files.newOutputStream(partPath);
                        return operation.getContent()
                                .doOnNext(buffer -> {
                                    try {
                                        byte[] bytes = new byte[buffer.remaining()];
                                        buffer.get(bytes);
                                        os.write(bytes);
                                        md5.update(bytes);
                                        totalSize.addAndGet(bytes.length);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .doFinally(signal -> {
                                    try {
                                        os.close();
                                    } catch (Exception ignored) {
                                    }
                                })
                                .then(Mono.fromCallable(() -> {
                                    String etag = "\"" + bytesToHex(md5.digest()) + "\"";
                                    Instant now = Instant.now();

                                    // Record part info
                                    state.getParts().put(partNumber, SftpDriverContext.PartInfo.builder()
                                            .partNumber(partNumber)
                                            .size(totalSize.get())
                                            .etag(etag)
                                            .lastModified(now)
                                            .build());

                                    return UploadPart.builder()
                                            .partNumber(partNumber)
                                            .etag(etag)
                                            .size(totalSize.get())
                                            .lastModified(now)
                                            .build();
                                }));
                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public Class<UploadPartOperation> getOperationType() {
        return UploadPartOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
