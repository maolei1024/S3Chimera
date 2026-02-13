package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 上传分片处理器
 */
public class MemoryUploadPartHandler implements OperationHandler<UploadPartOperation, UploadPart> {

    @Override
    public Mono<UploadPart> handle(UploadPartOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        return operation.getContent()
                .reduce(new ByteArrayOutputStream(), (baos, buf) -> {
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    baos.write(bytes, 0, bytes.length);
                    return baos;
                })
                .map(ByteArrayOutputStream::toByteArray)
                .map(data -> {
                    String etag = "\"" + calculateMd5(data) + "\"";
                    Instant now = Instant.now();

                    var partData = MemoryDriverContext.PartData.builder()
                            .data(data)
                            .etag(etag)
                            .lastModified(now)
                            .build();

                    state.getParts().put(partNumber, partData);

                    return UploadPart.builder()
                            .partNumber(partNumber)
                            .etag(etag)
                            .size((long) data.length)
                            .lastModified(now)
                            .build();
                });
    }

    private String calculateMd5(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
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
