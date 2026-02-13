package win.ixuni.chimera.driver.memory.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.ListObjectsRequest;
import win.ixuni.chimera.core.model.ListObjectsV2Result;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.memory.context.MemoryDriverContext;

import java.util.*;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * Memory ListObjects 处理器 (V2)
 */
@Slf4j
public class MemoryListObjectsV2Handler implements OperationHandler<ListObjectsV2Operation, ListObjectsV2Result> {

    @Override
    public Mono<ListObjectsV2Result> handle(ListObjectsV2Operation operation, DriverContext context) {
        MemoryDriverContext ctx = (MemoryDriverContext) context;
        ListObjectsRequest request = operation.getRequest();
        
        String bucketName = request.getBucketName();
        
        // 验证 bucket 是否存在
        if (!ctx.getBuckets().containsKey(bucketName)) {
            log.debug("ListObjectsV2: bucket not found: {}", bucketName);
            return Mono.error(new BucketNotFoundException(bucketName));
        }
        
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

        List<ListObjectsV2Result.CommonPrefix> commonPrefixes = commonPrefixStrings.stream()
                .map(p -> ListObjectsV2Result.CommonPrefix.builder().prefix(p).build())
                .toList();

        log.debug("ListObjectsV2: bucket={}, prefix={}, found {} objects", 
                bucketName, prefix, resultObjects.size());

        return Mono.just(ListObjectsV2Result.builder()
                .name(bucketName)
                .prefix(prefix)
                .delimiter(delimiter)
                .maxKeys(maxKeys)
                .isTruncated(isTruncated)
                .contents(resultObjects)
                .commonPrefixes(commonPrefixes)
                .keyCount(resultObjects.size())
                .build());
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

