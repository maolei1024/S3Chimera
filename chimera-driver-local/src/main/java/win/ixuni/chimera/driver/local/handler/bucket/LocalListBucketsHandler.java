package win.ixuni.chimera.driver.local.handler.bucket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;

import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统列出 Buckets 处理器
 */
public class LocalListBucketsHandler implements OperationHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    public Mono<Flux<S3Bucket>> handle(ListBucketsOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;

        return Mono.fromCallable(() -> {
            var dataRoot = ctx.getDataRoot();

            if (!Files.exists(dataRoot)) {
                return Flux.empty();
            }

            return Flux.fromStream(
                    Files.list(dataRoot)
                            .filter(Files::isDirectory)
                            .filter(path -> !path.getFileName().toString().startsWith("."))
                            .map(path -> {
                                try {
                                    var attrs = Files.readAttributes(path, BasicFileAttributes.class);
                                    return S3Bucket.builder()
                                            .name(path.getFileName().toString())
                                            .creationDate(attrs.creationTime().toInstant())
                                            .build();
                                } catch (Exception e) {
                                    return S3Bucket.builder()
                                            .name(path.getFileName().toString())
                                            .build();
                                }
                            }));
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
