package win.ixuni.chimera.driver.sftp.handler.bucket;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * SFTP 创建 Bucket 处理器
 * <p>
 * 在 SFTP 服务器上创建目录作为 Bucket
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
public class SftpCreateBucketHandler implements OperationHandler<CreateBucketOperation, S3Bucket> {

    @Override
    public Mono<S3Bucket> handle(CreateBucketOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);
                String bucketMetaPath = ctx.getBucketMetaPath(bucketName);

                // Idempotent: if bucket already exists, return existing bucket info
                try {
                    SftpClient.Attributes attrs = sftpClient.stat(bucketPath);
                    // Bucket already exists, return its info
                    long mtime = attrs.getModifyTime().toMillis();
                    return S3Bucket.builder()
                            .name(bucketName)
                            .creationDate(Instant.ofEpochMilli(mtime))
                            .build();
                } catch (java.io.IOException e) {
                    // 不存在，继续创建
                }

                // 确保 s3chimera 根目录存在
                ensureDirectoryExists(sftpClient, ctx.getDataRoot());
                ensureDirectoryExists(sftpClient, ctx.getMetaRoot());

                // 创建 data 和 meta bucket 目录
                sftpClient.mkdir(bucketPath);
                sftpClient.mkdir(bucketMetaPath);

                return S3Bucket.builder()
                        .name(bucketName)
                        .creationDate(Instant.now())
                        .build();
            }
        });
    }

    private void ensureDirectoryExists(SftpClient client, String path) throws java.io.IOException {
        try {
            client.stat(path);
        } catch (java.io.IOException e) {
            // 父目录可能也不存在，先递归创建
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash > 0) {
                ensureDirectoryExists(client, path.substring(0, lastSlash));
            }
            try {
                client.mkdir(path);
            } catch (java.io.IOException ignored) {
                // May have been concurrently created
            }
        }
    }

    @Override
    public Class<CreateBucketOperation> getOperationType() {
        return CreateBucketOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE);
    }
}
