package win.ixuni.chimera.driver.webdav.handler.object;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * WebDAV 列出对象处理器 (V1)
 * Uses depth=1 traversal, compatible with WebDAV servers that do not support infinite depth
 */
public class WebDavListObjectsHandler implements OperationHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    public Mono<ListObjectsResult> handle(ListObjectsOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        var request = operation.getRequest();
        String bucketName = request.getBucketName();

        return Mono.fromCallable(() -> {
            String bucketUrl = ctx.getBucketUrl(bucketName);

            // 先检查 bucket 是否存在
            if (!ctx.getSardine().exists(bucketUrl)) {
                throw new BucketNotFoundException(bucketName);
            }

            // Use depth=1 to traverse all files
            List<DavResource> allResources = new ArrayList<>();
            listRecursively(ctx.getSardine(), bucketUrl, bucketUrl, allResources);

            String prefix = request.getPrefix() != null ? request.getPrefix() : "";
            String delimiter = request.getDelimiter();
            String marker = request.getMarker();
            int maxKeys = request.getMaxKeys() != null ? request.getMaxKeys() : 1000;

            Set<String> commonPrefixes = new TreeSet<>();
            List<S3Object> contents = new ArrayList<>();

            boolean truncated = false;

            for (DavResource resource : allResources) {
                if (resource.isDirectory())
                    continue;

                String key = extractKey(bucketUrl, resource);
                if (key.isEmpty() || key.endsWith(SidecarMetadata.getSidecarSuffix()))
                    continue;

                if (!key.startsWith(prefix))
                    continue;
                if (marker != null && key.compareTo(marker) <= 0)
                    continue;

                if (delimiter != null && !delimiter.isEmpty()) {
                    String afterPrefix = key.substring(prefix.length());
                    int delimIndex = afterPrefix.indexOf(delimiter);
                    if (delimIndex >= 0) {
                        commonPrefixes.add(prefix + afterPrefix.substring(0, delimIndex + 1));
                        continue;
                    }
                }

                if (contents.size() >= maxKeys) {
                    truncated = true;
                    break;
                }

                contents.add(S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size(resource.getContentLength() != null ? resource.getContentLength() : 0L)
                        .etag(resource.getEtag())
                        .lastModified(resource.getModified() != null ? resource.getModified().toInstant() : null)
                        .build());
            }

            String nextMarker = truncated && !contents.isEmpty()
                    ? contents.get(contents.size() - 1).getKey()
                    : null;

            return ListObjectsResult.builder()
                    .bucketName(bucketName)
                    .prefix(prefix)
                    .delimiter(delimiter)
                    .isTruncated(truncated)
                    .nextMarker(nextMarker)
                    .contents(contents)
                    .commonPrefixes(commonPrefixes.stream()
                            .map(p -> ListObjectsResult.CommonPrefix.builder().prefix(p).build())
                            .toList())
                    .build();
        });
    }

    /**
     * Uses depth=1 to list all resources
     */
    private void listRecursively(Sardine sardine, String baseUrl, String currentUrl, List<DavResource> result)
            throws IOException {
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
                String subUrl = resolveUrl(baseUrl, href);
                listRecursively(sardine, baseUrl, subUrl, result);
            } else {
                result.add(resource);
            }
        }
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

    /**
     * 从 resource 提取对象 key
     */
    private String extractKey(String bucketUrl, DavResource resource) {
        String path = resource.getPath();
        // 解码路径
        String decodedPath = java.net.URLDecoder.decode(path, java.nio.charset.StandardCharsets.UTF_8);

        // 从 bucket URL 提取 bucket 路径
        try {
            URI bucketUri = URI.create(bucketUrl);
            String bucketPath = bucketUri.getPath();
            if (bucketPath.endsWith("/")) {
                bucketPath = bucketPath.substring(0, bucketPath.length() - 1);
            }

            if (decodedPath.startsWith(bucketPath + "/")) {
                return decodedPath.substring(bucketPath.length() + 1);
            }
            if (decodedPath.startsWith(bucketPath)) {
                return decodedPath.substring(bucketPath.length());
            }
        } catch (Exception e) {
            // fall through
        }

        return decodedPath;
    }

    @Override
    public Class<ListObjectsOperation> getOperationType() {
        return ListObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
