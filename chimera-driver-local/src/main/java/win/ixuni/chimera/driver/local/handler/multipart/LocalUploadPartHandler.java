package win.ixuni.chimera.driver.local.handler.multipart;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.UploadPartOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local 上传分片处理器
 * <p>
 * 流式写入文件，不加载到内存，避免 OOM
 */
public class LocalUploadPartHandler implements OperationHandler<UploadPartOperation, UploadPart> {

    @Override
    public Mono<UploadPart> handle(UploadPartOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String uploadId = operation.getUploadId();
        int partNumber = operation.getPartNumber();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            return Mono.error(new IllegalArgumentException("Invalid uploadId: " + uploadId));
        }

        Path partPath = ctx.getPartPath(state.getBucketName(), uploadId, partNumber);
        AtomicLong totalSize = new AtomicLong(0);

        // 流式写入文件，不加载到内存
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
                                    String etag = "\"" + LocalFileUtils.bytesToHex(md5.digest()) + "\"";
                                    Instant now = Instant.now();

                                    // Record part info
                                    var partInfo = LocalDriverContext.PartInfo.builder()
                                            .partNumber(partNumber)
                                            .size(totalSize.get())
                                            .etag(etag)
                                            .lastModified(now)
                                            .build();
                                    state.getParts().put(partNumber, partInfo);

                                    // 持久化分片状态
                                    ctx.persistMultipartState(state);

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

    @Override
    public Class<UploadPartOperation> getOperationType() {
        return UploadPartOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.MULTIPART_UPLOAD);
    }
}
