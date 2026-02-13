package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * 本地文件系统列出对象处理器 (V1)
 */
public class LocalListObjectsHandler implements OperationHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    public Mono<ListObjectsResult> handle(ListObjectsOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        return Mono.fromCallable(() -> {
            var bucketPath = ctx.getBucketPath(bucketName);

            if (!Files.exists(bucketPath)) {
                return ListObjectsResult.builder()
                        .bucketName(bucketName)
                        .contents(List.of())
                        .isTruncated(false)
                        .build();
            }

            String prefix = request.getPrefix() != null ? request.getPrefix() : "";
            String delimiter = request.getDelimiter();
            String marker = request.getMarker();
            int maxKeys = request.getMaxKeys() != null ? request.getMaxKeys() : 1000;

            List<S3Object> allMatching = new ArrayList<>();
            Set<String> commonPrefixes = new TreeSet<>();

            try (Stream<Path> stream = Files.walk(bucketPath)) {
                stream.filter(Files::isRegularFile)
                        // 过滤 .multipart 临时文件
                        .filter(path -> !LocalFileUtils.isMultipartTempFile(bucketPath, path))
                        .forEach(path -> {
                            String key = bucketPath.relativize(path).toString().replace("\\", "/");

                            if (!key.startsWith(prefix))
                                return;
                            if (marker != null && key.compareTo(marker) <= 0)
                                return;

                            // 处理 delimiter
                            if (delimiter != null && !delimiter.isEmpty()) {
                                String afterPrefix = key.substring(prefix.length());
                                int delimIndex = afterPrefix.indexOf(delimiter);
                                if (delimIndex >= 0) {
                                    commonPrefixes.add(prefix + afterPrefix.substring(0, delimIndex + 1));
                                    return;
                                }
                            }

                            try {
                                var metaPath = ctx.getMetadataPath(bucketName, key);
                                SidecarMetadata sidecar = null;
                                if (Files.exists(metaPath)) {
                                    sidecar = LocalFileUtils.MAPPER.readValue(Files.readString(metaPath),
                                            SidecarMetadata.class);
                                }

                                var attrs = Files.readAttributes(path,
                                        java.nio.file.attribute.BasicFileAttributes.class);
                                allMatching.add(S3Object.builder()
                                        .bucketName(bucketName)
                                        .key(key)
                                        .size(attrs.size())
                                        .etag(sidecar != null ? sidecar.getEtag() : null)
                                        .lastModified(attrs.lastModifiedTime().toInstant())
                                        .build());
                            } catch (Exception ignored) {
                            }
                        });
            }

            // 先排序再截断，确保返回字典序最小的 N 个对象
            allMatching.sort(Comparator.comparing(S3Object::getKey));

            boolean isTruncated = allMatching.size() > maxKeys;
            List<S3Object> contents = isTruncated
                    ? allMatching.subList(0, maxKeys)
                    : allMatching;

            // 计算 nextMarker
            String nextMarker = null;
            if (isTruncated && !contents.isEmpty()) {
                nextMarker = contents.get(contents.size() - 1).getKey();
            }

            return ListObjectsResult.builder()
                    .bucketName(bucketName)
                    .prefix(prefix)
                    .delimiter(delimiter)
                    .contents(new ArrayList<>(contents))
                    .commonPrefixes(commonPrefixes.stream()
                            .map(p -> ListObjectsResult.CommonPrefix.builder().prefix(p).build())
                            .toList())
                    .isTruncated(isTruncated)
                    .nextMarker(nextMarker)
                    .build();
        });
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
