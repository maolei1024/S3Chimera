package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectRangeOperation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统获取对象范围处理器
 */
public class LocalGetObjectRangeHandler implements OperationHandler<GetObjectRangeOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectRangeOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        long start = operation.getRangeStart();
        Long end = operation.getRangeEnd();

        return Mono.fromCallable(() -> {
            var objectPath = ctx.getObjectPath(bucketName, key);
            var metaPath = ctx.getMetadataPath(bucketName, key);

            if (!Files.exists(objectPath)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            long fileSize = Files.size(objectPath);

            // 处理 suffix range：当 start 为负数时，表示文件末尾的 N 字节
            long effectiveStart;
            long effectiveEnd;
            if (start < 0) {
                // suffix range: bytes=-N → 返回文件末尾 N 字节
                long suffixLength = Math.min(-start, fileSize);
                effectiveStart = fileSize - suffixLength;
                effectiveEnd = fileSize - 1;
            } else {
                effectiveStart = start;
                effectiveEnd = (end == null || end >= fileSize) ? fileSize - 1 : end;
            }

            // 边界校验
            if (effectiveStart >= fileSize || effectiveStart > effectiveEnd) {
                throw new IllegalArgumentException(
                        "Invalid range: " + effectiveStart + "-" + effectiveEnd + " for file size " + fileSize);
            }

            int rangeLength = (int) (effectiveEnd - effectiveStart + 1);

            // 读取指定范围
            byte[] rangeData = new byte[rangeLength];
            try (RandomAccessFile raf = new RandomAccessFile(objectPath.toFile(), "r")) {
                raf.seek(effectiveStart);
                raf.readFully(rangeData);
            }

            // 读取 Sidecar 元数据
            SidecarMetadata sidecar = LocalFileUtils.readSidecar(objectPath, metaPath);

            S3Object s3Object = S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size((long) rangeLength)
                    .etag(sidecar.getEtag())
                    .lastModified(sidecar.getLastModifiedInstant())
                    .contentType(sidecar.getContentType())
                    .build();

            return S3ObjectData.builder()
                    .metadata(s3Object)
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
        return EnumSet.of(Capability.READ);
    }
}
