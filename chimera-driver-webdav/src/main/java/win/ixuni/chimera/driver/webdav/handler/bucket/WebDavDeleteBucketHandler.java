package win.ixuni.chimera.driver.webdav.handler.bucket;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotEmptyException;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.bucket.DeleteBucketOperation;

import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * WebDAV 删除 Bucket 处理器
 * Uses depth=1 to check if empty, compatible with WebDAV servers that do not support infinite depth
 */
public class WebDavDeleteBucketHandler implements OperationHandler<DeleteBucketOperation, Void> {

    @Override
    public Mono<Void> handle(DeleteBucketOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        String bucketName = operation.getBucketName();

        return Mono.fromRunnable(() -> {
            try {
                String bucketDataUrl = ctx.getBucketUrl(bucketName);
                String bucketMetaUrl = ctx.getBucketMetaUrl(bucketName);

                if (!ctx.getSardine().exists(bucketDataUrl)) {
                    throw new BucketNotFoundException(bucketName);
                }

                // 检查 data 目录是否真正为空（不包括空子目录）
                if (hasFiles(ctx.getSardine(), bucketDataUrl)) {
                    throw new BucketNotEmptyException(bucketName);
                }

                // 删除 data bucket 目录
                ctx.getSardine().delete(bucketDataUrl);

                // 删除 meta bucket 目录（如果存在）
                if (ctx.getSardine().exists(bucketMetaUrl)) {
                    ctx.getSardine().delete(bucketMetaUrl);
                }
            } catch (BucketNotFoundException | BucketNotEmptyException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete bucket: " + bucketName, e);
            }
        });
    }

    /**
     * Uses depth=1 to check if directory contains any files
     */
    private boolean hasFiles(Sardine sardine, String currentUrl) throws IOException {
        List<DavResource> resources = sardine.list(currentUrl, 1);

        // Skip the first entry (the directory itself)
        boolean first = true;
        for (DavResource resource : resources) {
            if (first) {
                first = false;
                continue;
            }

            if (resource.isDirectory()) {
                // Use href to build full URL for recursive traversal
                URI href = resource.getHref();
                String subUrl = resolveUrl(currentUrl, href);
                if (hasFiles(sardine, subUrl)) {
                    return true;
                }
            } else {
                // 发现文件
                return true;
            }
        }
        return false;
    }

    /**
     * 根据 base URL 和相对 href 构建完整 URL
     */
    private String resolveUrl(String baseUrl, URI href) {
        String hrefStr = href.toString();
        // 如果 href 是完整 URL，直接返回
        if (hrefStr.startsWith("http://") || hrefStr.startsWith("https://")) {
            return hrefStr.endsWith("/") ? hrefStr : hrefStr + "/";
        }
        // Otherwise parse based on base URL
        try {
            URI base = URI.create(baseUrl);
            URI resolved = base.resolve(href);
            String result = resolved.toString();
            return result.endsWith("/") ? result : result + "/";
        } catch (Exception e) {
            return hrefStr.endsWith("/") ? hrefStr : hrefStr + "/";
        }
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
