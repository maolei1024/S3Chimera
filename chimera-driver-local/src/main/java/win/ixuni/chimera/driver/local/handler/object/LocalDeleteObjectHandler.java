package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统删除对象处理器
 */
public class LocalDeleteObjectHandler implements OperationHandler<DeleteObjectOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteObjectOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromRunnable(() -> {
            try {
                var bucketPath = ctx.getBucketPath(bucketName);

                // Check bucket exists first
                if (!Files.exists(bucketPath)) {
                    throw new BucketNotFoundException(bucketName);
                }

                var objectPath = ctx.getObjectPath(bucketName, key);
                var metaPath = ctx.getMetadataPath(bucketName, key);

                // Delete file and metadata (ignore non-existent, per S3 semantics)
                Files.deleteIfExists(objectPath);
                Files.deleteIfExists(metaPath);
            } catch (BucketNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete object: " + bucketName + "/" + key, e);
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
