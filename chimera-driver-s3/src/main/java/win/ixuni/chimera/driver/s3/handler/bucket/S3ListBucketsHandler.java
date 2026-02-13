package win.ixuni.chimera.driver.s3.handler.bucket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.ListBucketsOperation;
import win.ixuni.chimera.driver.s3.context.S3ProxyDriverContext;

import java.util.EnumSet;
import java.util.Set;

/**
 * S3 代理列出 Buckets 处理器
 */
public class S3ListBucketsHandler implements OperationHandler<ListBucketsOperation, Flux<S3Bucket>> {

    @Override
    public Mono<Flux<S3Bucket>> handle(ListBucketsOperation operation, DriverContext context) {
        S3ProxyDriverContext ctx = (S3ProxyDriverContext) context;

        return Mono.fromFuture(() -> ctx.getS3Client().listBuckets(ListBucketsRequest.builder().build()))
                .map(response -> Flux.fromIterable(response.buckets())
                        .map(bucket -> S3Bucket.builder()
                                .name(bucket.name())
                                .creationDate(bucket.creationDate())
                                .build()));
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
