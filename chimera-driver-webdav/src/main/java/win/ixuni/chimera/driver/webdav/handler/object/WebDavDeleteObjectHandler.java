package win.ixuni.chimera.driver.webdav.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * WebDAV 删除对象处理器
 */
public class WebDavDeleteObjectHandler implements OperationHandler<DeleteObjectOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteObjectOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromRunnable(() -> {
            try {
                // 先检查 bucket 是否存在
                String bucketUrl = ctx.getBucketUrl(bucketName);
                if (!ctx.getSardine().exists(bucketUrl)) {
                    throw new BucketNotFoundException(bucketName);
                }

                String objectUrl = ctx.getObjectUrl(bucketName, operation.getKey());

                // S3 semantics: deleting a non-existent object does not produce an error
                if (ctx.getSardine().exists(objectUrl)) {
                    ctx.getSardine().delete(objectUrl);
                }

                // 同时尝试删除 Sidecar 文件（从隔离的 meta 目录）
                String metadataUrl = ctx.getMetadataUrl(bucketName, operation.getKey());
                if (ctx.getSardine().exists(metadataUrl)) {
                    ctx.getSardine().delete(metadataUrl);
                }
            } catch (BucketNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete object", e);
            }
        });
    }

    @Override
    public Class<DeleteObjectOperation> getOperationType() {
        return DeleteObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
