package win.ixuni.chimera.driver.memory.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.util.*;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory ListObjects 处理器 (V1)
 */
public class MemoryListObjectsHandler implements OperationHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    public Mono<ListObjectsResult> handle(ListObjectsOperation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        ListObjectsRequest request = operation.getRequest();
        
        String bucketName = request.getBucketName();
        String prefix = request.getPrefix() != null ? request.getPrefix() : "";
        String delimiter = request.getDelimiter();
        int maxKeys = request.getMaxKeys() != null ? request.getMaxKeys() : 1000;

        List<S3Object> objects = new ArrayList<>();
        Set<String> commonPrefixStrings = new TreeSet<>();

        ctx.getObjects().forEach((objKey, objData) -> {
            if (!objKey.startsWith(bucketName + "/"))
                return;
            String key = objKey.substring(bucketName.length() + 1);
            if (!key.startsWith(prefix))
                return;

            if (delimiter != null && !delimiter.isEmpty()) {
                String afterPrefix = key.substring(prefix.length());
                int delimIndex = afterPrefix.indexOf(delimiter);
                if (delimIndex >= 0) {
                    commonPrefixStrings.add(prefix + afterPrefix.substring(0, delimIndex + 1));
                    return;
                }
            }

            objects.add(S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size((long) objData.getData().length)
                    .etag(objData.getEtag())
                    .lastModified(objData.getLastModified())
                    .storageClass("STANDARD")
                    .build());
        });

        objects.sort(Comparator.comparing(S3Object::getKey));
        boolean isTruncated = objects.size() > maxKeys;
        List<S3Object> resultObjects = objects.subList(0, Math.min(maxKeys, objects.size()));

        List<ListObjectsResult.CommonPrefix> commonPrefixes = commonPrefixStrings.stream()
                .map(p -> ListObjectsResult.CommonPrefix.builder().prefix(p).build())
                .toList();

        return Mono.just(ListObjectsResult.builder()
                .bucketName(bucketName)
                .prefix(prefix)
                .delimiter(delimiter)
                .isTruncated(isTruncated)
                .contents(resultObjects)
                .commonPrefixes(commonPrefixes)
                .build());
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
