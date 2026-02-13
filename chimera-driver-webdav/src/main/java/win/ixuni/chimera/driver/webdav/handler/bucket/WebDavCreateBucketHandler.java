package win.ixuni.chimera.driver.webdav.handler.bucket;

import com.github.sardine.DavResource;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.CreateBucketOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * WebDAV 创建 Bucket 处理器
 * <p>
 * Follows AWS S3 idempotent behavior: if bucket already exists and belongs to current user, return 200 OK
 */
public class WebDavCreateBucketHandler implements OperationHandler<CreateBucketOperation, S3Bucket> {

    @Override
    public Mono<S3Bucket> handle(CreateBucketOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromCallable(() -> {
            String bucketUrl = ctx.getBucketUrl(bucketName);
            String bucketMetaUrl = ctx.getBucketMetaUrl(bucketName);

            // Idempotent: if bucket already exists, return existing bucket info
            if (ctx.getSardine().exists(bucketUrl)) {
                List<DavResource> resources = ctx.getSardine().list(bucketUrl, 0);
                if (!resources.isEmpty()) {
                    DavResource resource = resources.get(0);
                    Instant creationDate = resource.getModified() != null
                            ? resource.getModified().toInstant()
                            : Instant.now();
                    return S3Bucket.builder()
                            .name(bucketName)
                            .creationDate(creationDate)
                            .build();
                }
            }

            // 确保 s3chimera 根目录存在
            ensureDirectoryExists(ctx, ctx.getDataRootUrl());
            ensureDirectoryExists(ctx, ctx.getMetaRootUrl());

            // 创建 data 和 meta bucket 目录
            ctx.getSardine().createDirectory(bucketUrl);
            ctx.getSardine().createDirectory(bucketMetaUrl);

            return S3Bucket.builder()
                    .name(bucketName)
                    .creationDate(Instant.now())
                    .build();
        });
    }

    private void ensureDirectoryExists(WebDavDriverContext ctx, String dirUrl) throws Exception {
        if (!ctx.getSardine().exists(dirUrl)) {
            // 父目录可能也不存在，先递归创建
            int lastSlash = dirUrl.substring(0, dirUrl.length() - 1).lastIndexOf('/');
            if (lastSlash > 0) {
                ensureDirectoryExists(ctx, dirUrl.substring(0, lastSlash + 1));
            }
            try {
                ctx.getSardine().createDirectory(dirUrl);
            } catch (Exception ignored) {
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
