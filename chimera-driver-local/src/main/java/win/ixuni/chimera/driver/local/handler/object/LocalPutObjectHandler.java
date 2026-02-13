package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.io.OutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地文件系统上传对象处理器
 * <p>
 * 流式写入文件，避免大文件 OOM
 */
public class LocalPutObjectHandler implements OperationHandler<PutObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(PutObjectOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            var bucketPath = ctx.getBucketPath(bucketName);
            if (!Files.exists(bucketPath)) {
                throw new BucketNotFoundException(bucketName);
            }

            var objectPath = ctx.getObjectPath(bucketName, key);
            var metaPath = ctx.getMetadataPath(bucketName, key);

            // 创建父目录（支持嵌套 key）
            Files.createDirectories(objectPath.getParent());
            Files.createDirectories(metaPath.getParent());

            return MessageDigest.getInstance("MD5");
        }).flatMap(md5 -> {
            var objectPath = ctx.getObjectPath(bucketName, key);
            AtomicLong totalSize = new AtomicLong(0);

            try {
                OutputStream os = Files.newOutputStream(objectPath);
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
                            String etag = "\"" + LocalFileUtils.bytesToHex(md5.digest()) + "\"";
                            Instant now = Instant.now();

                            // 写入 Sidecar 元数据
                            var metaPath = ctx.getMetadataPath(bucketName, key);
                            var sidecar = SidecarMetadata.builder()
                                    .contentType(operation.getContentType())
                                    .size(totalSize.get())
                                    .etag(etag)
                                    .lastModified(now.toEpochMilli())
                                    .userMetadata(operation.getMetadata())
                                    .build();
                            LocalFileUtils.writeSidecar(metaPath, sidecar);

                            return S3Object.builder()
                                    .bucketName(bucketName)
                                    .key(key)
                                    .size(totalSize.get())
                                    .etag(etag)
                                    .lastModified(now)
                                    .contentType(operation.getContentType())
                                    .userMetadata(operation.getMetadata())
                                    .build();
                        }));
            } catch (Exception e) {
                return Mono.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
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
