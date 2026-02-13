package win.ixuni.chimera.driver.local.handler.multipart;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;

/**
 * Local 中止分片上传处理器
 */
public class LocalAbortMultipartUploadHandler
        implements OperationHandler<AbortMultipartUploadOperation, Void> {

    @Override
    public Mono<Void> handle(AbortMultipartUploadOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String uploadId = operation.getUploadId();

        var state = ctx.getMultipartUploads().get(uploadId);
        if (state == null) {
            // 如果不存在，静默返回成功（S3 行为）
            return Mono.empty();
        }

        return Mono.fromRunnable(() -> {
            try {
                // 删除临时分片目录
                Path multipartDir = ctx.getMultipartPath(state.getBucketName(), uploadId);
                LocalFileUtils.deleteDirectoryRecursively(multipartDir);

                // 删除持久化状态文件
                ctx.removeMultipartState(uploadId);
            } catch (Exception e) {
                throw new RuntimeException("Failed to abort multipart upload", e);
            }
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
