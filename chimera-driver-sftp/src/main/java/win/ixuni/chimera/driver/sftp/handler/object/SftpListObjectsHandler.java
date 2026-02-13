package win.ixuni.chimera.driver.sftp.handler.object;

import org.apache.sshd.sftp.client.SftpClient;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.io.IOException;
import java.util.*;
import win.ixuni.chimera.core.util.SidecarMetadata;

/**
 * SFTP 列出对象处理器 (V1)
 */
public class SftpListObjectsHandler implements OperationHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    public Mono<ListObjectsResult> handle(ListObjectsOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;
        var request = operation.getRequest();
        String bucketName = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        String marker = request.getMarker();
        int maxKeys = request.getMaxKeys() != null ? request.getMaxKeys() : 1000;

        return Mono.fromCallable(() -> {
            try (SftpClient sftpClient = ctx.createSftpClient()) {
                String bucketPath = ctx.getBucketPath(bucketName);

                // 检查 bucket 是否存在
                try {
                    sftpClient.stat(bucketPath);
                } catch (IOException e) {
                    throw new BucketNotFoundException(bucketName);
                }

                List<S3Object> objects = new ArrayList<>();
                Set<String> commonPrefixes = new TreeSet<>();

                // Recursively list all files
                listRecursive(sftpClient, bucketPath, "", prefix, delimiter, marker,
                        maxKeys, objects, commonPrefixes, bucketName, ctx);

                // 排序
                objects.sort(Comparator.comparing(S3Object::getKey));

                // 分页
                boolean isTruncated = objects.size() > maxKeys;
                if (isTruncated) {
                    objects = objects.subList(0, maxKeys);
                }

                String nextMarker = isTruncated ? objects.get(objects.size() - 1).getKey() : null;

                return ListObjectsResult.builder()
                        .bucketName(bucketName)
                        .prefix(prefix)
                        .delimiter(delimiter)
                        .isTruncated(isTruncated)
                        .nextMarker(nextMarker)
                        .contents(objects)
                        .commonPrefixes(commonPrefixes.stream()
                                .map(p -> ListObjectsResult.CommonPrefix.builder().prefix(p).build())
                                .toList())
                        .build();
            }
        });
    }

    private void listRecursive(SftpClient sftpClient, String bucketPath, String currentPath,
            String prefix, String delimiter, String marker, int maxKeys,
            List<S3Object> objects, Set<String> commonPrefixes, String bucketName, SftpDriverContext ctx)
            throws IOException {
        String fullPath = currentPath.isEmpty() ? bucketPath : bucketPath + "/" + currentPath;

        // Use readDir(path) instead of readDir(handle)
        // readDir(path) returns Iterable, automatically handles SFTP protocol
        // pagination
        // readDir(handle) may return partial results, requiring multiple calls
        for (var entry : sftpClient.readDir(fullPath)) {
            String name = entry.getFilename();
            if (".".equals(name) || "..".equals(name) || name.endsWith(SidecarMetadata.getSidecarSuffix()))
                continue;

            String key = currentPath.isEmpty() ? name : currentPath + "/" + name;

            // Apply prefix filter
            if (!prefix.isEmpty() && !key.startsWith(prefix)) {
                if (entry.getAttributes().isDirectory() && prefix.startsWith(key + "/")) {
                    // 递归进入可能匹配前缀的目录
                    listRecursive(sftpClient, bucketPath, key, prefix, delimiter,
                            marker, maxKeys, objects, commonPrefixes, bucketName, ctx);
                }
                continue;
            }

            // Apply marker filter
            if (marker != null && key.compareTo(marker) <= 0) {
                if (entry.getAttributes().isDirectory()) {
                    listRecursive(sftpClient, bucketPath, key, prefix, delimiter,
                            marker, maxKeys, objects, commonPrefixes, bucketName, ctx);
                }
                continue;
            }

            if (entry.getAttributes().isDirectory()) {
                if (delimiter != null && !delimiter.isEmpty()) {
                    // When delimiter is present, directories become CommonPrefixes
                    commonPrefixes.add(key + delimiter);
                } else {
                    // 无分隔符时，递归列出
                    listRecursive(sftpClient, bucketPath, key, prefix, delimiter,
                            marker, maxKeys, objects, commonPrefixes, bucketName, ctx);
                }
            } else {
                var attrs = entry.getAttributes();
                objects.add(S3Object.builder()
                        .bucketName(bucketName)
                        .key(key)
                        .size(attrs.getSize())
                        .etag(ctx.generateETag(attrs))
                        .lastModified(attrs.getModifyTime() != null
                                ? attrs.getModifyTime().toInstant()
                                : null)
                        .build());
            }

        }
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
