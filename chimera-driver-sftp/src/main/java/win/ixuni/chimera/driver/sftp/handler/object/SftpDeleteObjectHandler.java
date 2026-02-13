package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * SFTP 删除对象处理器
 */
public class SftpDeleteObjectHandler implements OperationHandler<DeleteObjectOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteObjectOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);

                // 检查 bucket 是否存在
                try {
                    sftpClient.stat(bucketPath);
                } catch (IOException e) {
                    throw new BucketNotFoundException(bucketName);
                }

                String objectPath = ctx.getObjectPath(bucketName, key);

                // 删除文件（如果存在）- S3 删除是幂等的
                try {
                    sftpClient.remove(objectPath);
                } catch (IOException e) {
                    // File does not exist, ignore (S3 semantics)
                }

                // 同时尝试删除 Sidecar 文件（从隔离的 meta 目录）
                try {
                    sftpClient.remove(ctx.getMetadataPath(bucketName, key));
                } catch (IOException e) {
                    // 忽略
                }

                return null;
            }
        }).then();
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
