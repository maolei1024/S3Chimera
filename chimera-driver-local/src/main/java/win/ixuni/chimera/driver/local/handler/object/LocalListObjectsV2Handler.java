package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * 本地文件系统列出对象处理器 (V2)
 */
public class LocalListObjectsV2Handler implements OperationHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    public Mono<ListObjectsV2Result> handle(ListObjectsV2Operation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        ListObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        return Mono.fromCallable(() -> {
            var bucketPath = ctx.getBucketPath(bucketName);

            if (!Files.exists(bucketPath)) {
                throw new BucketNotFoundException(bucketName);
            }

            String prefix = request.getPrefix() != null ? request.getPrefix() : "";
            String delimiter = request.getDelimiter();
            String startAfter = request.getStartAfter();
            // Support continuationToken (using key as token)
            String continuationToken = request.getContinuationToken();
            String effectiveStartAfter = continuationToken != null ? continuationToken : startAfter;
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
                            if (effectiveStartAfter != null && key.compareTo(effectiveStartAfter) <= 0)
                                return;

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

            // 先排序再截断
            allMatching.sort(Comparator.comparing(S3Object::getKey));

            boolean isTruncated = allMatching.size() > maxKeys;
            List<S3Object> contents = isTruncated
                    ? new ArrayList<>(allMatching.subList(0, maxKeys))
                    : allMatching;

            // 计算 nextContinuationToken
            String nextContinuationToken = null;
            if (isTruncated && !contents.isEmpty()) {
                nextContinuationToken = contents.get(contents.size() - 1).getKey();
            }

            return ListObjectsV2Result.builder()
                    .name(bucketName)
                    .prefix(prefix)
                    .delimiter(delimiter)
                    .startAfter(startAfter)
                    .continuationToken(continuationToken)
                    .nextContinuationToken(nextContinuationToken)
                    .maxKeys(maxKeys)
                    .keyCount(contents.size())
                    .contents(contents)
                    .commonPrefixes(commonPrefixes.stream()
                            .map(p -> ListObjectsV2Result.CommonPrefix.builder().prefix(p).build())
                            .toList())
                    .isTruncated(isTruncated)
                    .build();
        });
    }

    @Override
    public Class<ListObjectsV2Operation> getOperationType() {
        return ListObjectsV2Operation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
