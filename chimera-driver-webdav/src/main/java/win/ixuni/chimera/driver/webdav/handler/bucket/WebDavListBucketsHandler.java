package win.ixuni.chimera.driver.webdav.handler.bucket;

import com.github.sardine.DavResource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * WebDAV 列出 Buckets 处理器
 */
public class WebDavListBucketsHandler implements OperationHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    public Mono<Flux<S3Bucket>> handle(ListBucketsOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;

        return Mono.fromCallable(() -> {
            try {
                String dataRootUrl = ctx.getDataRootUrl();

                // 确保 data root 存在
                if (!ctx.getSardine().exists(dataRootUrl)) {
                    return Flux.<S3Bucket>empty();
                }

                var resources = ctx.getSardine().list(dataRootUrl, 1);

                return Flux.fromIterable(resources)
                        .skip(1) // skip the first entry (the directory itself)
                        .filter(DavResource::isDirectory)
                        .map(r -> S3Bucket.builder()
                                .name(r.getName())
                                .creationDate(r.getCreation() != null ? r.getCreation().toInstant() : null)
                                .build());
            } catch (Exception e) {
                return Flux.<S3Bucket>empty();
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
