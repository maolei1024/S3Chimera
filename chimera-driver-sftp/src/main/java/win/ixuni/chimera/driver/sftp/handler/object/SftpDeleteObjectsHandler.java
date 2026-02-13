package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * SFTP 批量删除对象处理器
 */
public class SftpDeleteObjectsHandler implements OperationHandler<DeleteObjectsOperation, DeleteObjectsResult> {

    @Override
    public Mono<DeleteObjectsResult> handle(DeleteObjectsOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        DeleteObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);

                // 检查 bucket 是否存在
                try {
                    sftpClient.stat(bucketPath);
                } catch (IOException e) {
                    throw new BucketNotFoundException(bucketName);
                }

                List<DeleteObjectsResult.DeletedObject> deleted = new ArrayList<>();
                List<DeleteObjectsResult.DeleteError> errors = new ArrayList<>();

                for (String key : request.getKeys()) {
                    try {
                        String objectPath = ctx.getObjectPath(bucketName, key);
                        sftpClient.remove(objectPath);
                        deleted.add(DeleteObjectsResult.DeletedObject.builder().key(key).build());
                    } catch (IOException e) {
                        // File does not exist counts as successful delete (S3 semantics)
                        deleted.add(DeleteObjectsResult.DeletedObject.builder().key(key).build());
                    }

                    // 同时尝试删除 Sidecar 文件（从隔离的 meta 目录）
                    try {
                        sftpClient.remove(ctx.getMetadataPath(bucketName, key));
                    } catch (IOException ignored) {
                        // Sidecar 不存在或删除失败，忽略
                    }
                }

                return DeleteObjectsResult.builder()
                        .deleted(deleted)
                        .errors(errors.isEmpty() ? null : errors)
                        .build();
            }
        });
    }

    @Override
    public Class<DeleteObjectsOperation> getOperationType() {
        return DeleteObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.BATCH_DELETE);
    }
}
