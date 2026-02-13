package win.ixuni.chimera.driver.mongodb.handler.multipart;

import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.operation.multipart.AbortMultipartUploadOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.util.EnumSet;
import java.util.Set;

/**
 * MongoDB 取消分片上传处理器
 */
@Slf4j
public class MongoAbortMultipartUploadHandler extends AbstractMongoHandler<AbortMultipartUploadOperation, Void> {

    @Override
    protected Mono<Void> doHandle(AbortMultipartUploadOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();
        String uploadId = operation.getUploadId();

        // 1. Delete all chunks
        return Mono.from(context.getChunkCollection(bucketName, key)
                .deleteMany(Filters.eq("uploadId", uploadId)))
                // 2. Delete all parts
                .then(Mono.from(context.getMultipartPartCollection()
                        .deleteMany(Filters.eq("uploadId", uploadId))))
                // 3. 删除上传会话
                .then(Mono.from(context.getMultipartUploadCollection()
                        .deleteOne(Filters.eq("uploadId", uploadId))))
                .doOnSuccess(result -> log.debug("AbortMultipartUpload: uploadId={}", uploadId))
                .then();
    }

    @Override
    public Class<AbortMultipartUploadOperation> getOperationType() {
        return AbortMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
    }
}
