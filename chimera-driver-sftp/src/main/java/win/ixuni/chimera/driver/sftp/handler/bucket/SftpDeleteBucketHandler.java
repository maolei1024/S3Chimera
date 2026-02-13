package win.ixuni.chimera.driver.sftp.handler.bucket;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

/**
 * SFTP 删除 Bucket 处理器
 * <p>
 * 删除 SFTP 服务器上的目录
 */
public class SftpDeleteBucketHandler implements OperationHandler<DeleteBucketOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteBucketOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketDataPath = ctx.getBucketPath(bucketName);
                String bucketMetaPath = ctx.getBucketMetaPath(bucketName);

                // 检查是否存在
                try {
                    sftpClient.stat(bucketDataPath);
                } catch (IOException e) {
                    throw new BucketNotFoundException(bucketName);
                }

                // 检查 data 目录是否为空（不包括空子目录）
                if (hasFiles(sftpClient, bucketDataPath)) {
                    throw new BucketNotEmptyException(bucketName);
                }

                // 递归删除 data bucket 目录
                deleteRecursively(sftpClient, bucketDataPath);

                // 递归删除 meta bucket 目录（如果存在）
                try {
                    sftpClient.stat(bucketMetaPath);
                    deleteRecursively(sftpClient, bucketMetaPath);
                } catch (IOException ignored) {
                    // meta 目录不存在，忽略
                }

                return null;
            }
        }).then();
    }

    /**
     * 检查目录中是否包含文件（递归）
     */
    private boolean hasFiles(SftpClient client, String path) throws IOException {
        try (var handle = client.openDir(path)) {
            for (var entry : client.readDir(handle)) {
                String name = entry.getFilename();
                if (".".equals(name) || "..".equals(name))
                    continue;

                if (entry.getAttributes().isDirectory()) {
                    if (hasFiles(client, path + "/" + name)) {
                        return true;
                    }
                } else {
                    return true; // 发现文件
                }
            }
        }
        return false;
    }

    /**
     * Recursively delete directory and all its contents
     */
    private void deleteRecursively(SftpClient client, String path) throws IOException {
        try (var handle = client.openDir(path)) {
            for (var entry : client.readDir(handle)) {
                String name = entry.getFilename();
                if (".".equals(name) || "..".equals(name))
                    continue;

                String childPath = path + "/" + name;
                if (entry.getAttributes().isDirectory()) {
                    deleteRecursively(client, childPath);
                } else {
                    client.remove(childPath);
                }
            }
        }
        client.rmdir(path);
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
