package win.ixuni.chimera.driver.sftp.handler.bucket;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

/**
 * SFTP 检查 Bucket 是否存在处理器
 */
public class SftpBucketExistsHandler implements OperationHandler<BucketExistsOperation, Boolean> {

    @Override
    public Mono<Boolean> handle(BucketExistsOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);

                try {
                    var attrs = sftpClient.stat(bucketPath);
                    return attrs.isDirectory();
                } catch (IOException e) {
                    return false;
                }
            }
        });
    }

    @Override
    public Class<BucketExistsOperation> getOperationType() {
        return BucketExistsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
