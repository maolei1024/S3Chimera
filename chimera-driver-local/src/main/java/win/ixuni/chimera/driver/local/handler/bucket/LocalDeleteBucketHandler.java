package win.ixuni.chimera.driver.local.handler.bucket;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统删除 Bucket 处理器
 */
public class LocalDeleteBucketHandler implements OperationHandler<DeleteBucketOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteBucketOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromRunnable(() -> {
            try {
                var bucketDataPath = ctx.getBucketPath(bucketName);
                var bucketMetaPath = ctx.getBucketMetaPath(bucketName);

                if (!Files.exists(bucketDataPath)) {
                    throw new BucketNotFoundException(bucketName);
                }

                // Check if bucket is empty (exclude .multipart temp files/dirs)
                try (var stream = Files.walk(bucketDataPath)) {
                    boolean hasObjects = stream
                            .filter(Files::isRegularFile)
                            .filter(path -> !LocalFileUtils.isMultipartTempFile(bucketDataPath, path))
                            .findAny()
                            .isPresent();
                    if (hasObjects) {
                        throw new BucketNotEmptyException(bucketName);
                    }
                }

                // Delete data bucket directory
                LocalFileUtils.deleteDirectoryRecursively(bucketDataPath);

                // Delete meta bucket directory (if exists)
                if (Files.exists(bucketMetaPath)) {
                    LocalFileUtils.deleteDirectoryRecursively(bucketMetaPath);
                }
            } catch (BucketNotFoundException | BucketNotEmptyException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete bucket: " + bucketName, e);
            }
        });
    }

    @Override
    public Class<DeleteBucketOperation> getOperationType() {
        return DeleteBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
