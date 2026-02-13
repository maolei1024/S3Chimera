package win.ixuni.chimera.driver.memory.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.CompletedPart;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.CompleteMultipartUploadOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory 完成分片上传处理器
 */
public class MemoryCompleteMultipartUploadHandler
        implements OperationHandler<CompleteMultipartUploadOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CompleteMultipartUploadOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        // 按 partNumber 排序并合并数据
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        operation.getParts().stream()
                .sorted(Comparator.comparingInt(CompletedPart::getPartNumber))
                .forEach(part -> {
                    var partData = state.getParts().get(part.getPartNumber());
                    if (partData != null) {
                        baos.write(partData.getData(), 0, partData.getData().length);
                    }
                });

        byte[] data = baos.toByteArray();
        String etag = "\"" + calculateMd5(data) + "-" + operation.getParts().size() + "\"";
        Instant now = Instant.now();

        // 删除旧对象（如果存在）
        ctx.getObjects().remove(ctx.objectKey(bucketName, key));

        // 创建新对象
        Map<String, String> metadata = state.getMetadata() != null ? new HashMap<>(state.getMetadata())
                : new HashMap<>();

        ctx.getObjects().put(ctx.objectKey(bucketName, key),
                MemoryDriverContext.ObjectData.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .data(data)
                        .etag(etag)
                        .contentType(state.getContentType())
                        .lastModified(now)
                        .metadata(metadata)
                        .build());

        // 清理 multipart 状态
        ctx.getMultipartUploads().remove(uploadId);

        return Mono.just(S3Object.builder()
                .bucketName(bucketName)
                .key(key)
                .size((long) data.length)
                .etag(etag)
                .lastModified(now)
                .storageClass("STANDARD")
                .build());
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
    public Class<CompleteMultipartUploadOperation> getOperationType() {
        return CompleteMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
