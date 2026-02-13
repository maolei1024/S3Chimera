package win.ixuni.chimera.driver.sftp.handler.bucket;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * SFTP 列出 Buckets 处理器
 * <p>
 * List all directories under base-path as buckets
 */
public class SftpListBucketsHandler implements OperationHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    public Mono<Flux<S3Bucket>> handle(ListBucketsOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                try {
                    String dataRoot = ctx.getDataRoot();

                    List<S3Bucket> buckets = new ArrayList<>();

                    try (var handle = sftpClient.openDir(dataRoot)) {
                        for (var entry : sftpClient.readDir(handle)) {
                            String name = entry.getFilename();
                            if (".".equals(name) || "..".equals(name)) {
                                continue;
                            }
                            if (entry.getAttributes().isDirectory()) {
                                var modTime = entry.getAttributes().getModifyTime();
                                buckets.add(S3Bucket.builder()
                                        .name(name)
                                        .creationDate(modTime != null
                                                ? modTime.toInstant()
                                                : Instant.now())
                                        .build());
                            }
                        }
                    }

                    return Flux.fromIterable(buckets);
                } catch (Exception e) {
                    return Flux.<S3Bucket>empty();
                }
            }
        });
    }

    @Override
    public Class<ListBucketsOperation> getOperationType() {
        return ListBucketsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
