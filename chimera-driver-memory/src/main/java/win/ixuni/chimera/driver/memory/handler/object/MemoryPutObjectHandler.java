package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory PutObject 处理器
 */
public class MemoryPutObjectHandler implements OperationHandler<PutObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(PutObjectOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        if (!ctx.getBuckets().containsKey(bucketName)) {
            return Mono.error(new BucketNotFoundException(bucketName));
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

                    ctx.getObjects().put(ctx.objectKey(bucketName, key),
                            MemoryDriverContext.ObjectData.builder()
                                    .bucketName(bucketName)
                                    .key(key)
                                    .data(data)
                                    .etag(etag)
                                    .contentType(operation.getContentType() != null ? operation.getContentType()
                                            : "application/octet-stream")
                                    .lastModified(now)
                                    .metadata(operation.getMetadata() != null ? operation.getMetadata() : Map.of())
                                    .build());

                    return S3Object.builder()
                            .bucketName(bucketName)
                            .key(key)
                            .size((long) data.length)
                            .etag(etag)
                            .lastModified(now)
                            .storageClass("STANDARD")
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
    public Class<PutObjectOperation> getOperationType() {
        return PutObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
