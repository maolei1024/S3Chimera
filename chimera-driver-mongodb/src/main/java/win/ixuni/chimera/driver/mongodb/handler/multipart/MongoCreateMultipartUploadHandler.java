package win.ixuni.chimera.driver.mongodb.handler.multipart;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.MultipartUpload;
import win.ixuni.chimera.core.operation.bucket.BucketExistsOperation;
import win.ixuni.chimera.core.operation.multipart.CreateMultipartUploadOperation;
import win.ixuni.chimera.driver.mongodb.context.MongoDriverContext;
import win.ixuni.chimera.driver.mongodb.handler.AbstractMongoHandler;

import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * MongoDB 创建分片上传处理器
 */
@Slf4j
public class MongoCreateMultipartUploadHandler
        extends AbstractMongoHandler<CreateMultipartUploadOperation, MultipartUpload> {

    @Override
    protected Mono<MultipartUpload> doHandle(CreateMultipartUploadOperation operation, MongoDriverContext context) {
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        // 1. 检查 S3Bucket 是否存在
        return context.execute(new BucketExistsOperation(bucketName))
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new BucketNotFoundException(bucketName));
                    }

                    String uploadId = UUID.randomUUID().toString().replace("-", "");
                    Instant now = Instant.now();

                    Document doc = new Document()
                            .append("uploadId", uploadId)
                            .append("bucketName", bucketName)
                            .append("objectKey", key)
                            .append("contentType", operation.getContentType())
                            .append("storageClass", "STANDARD")
                            .append("initiatedAt", Date.from(now))
                            .append("status", 0);

                    return Mono.from(context.getMultipartUploadCollection().insertOne(doc))
                            .thenReturn(MultipartUpload.builder()
                                    .uploadId(uploadId)
                                    .bucketName(bucketName)
                                    .key(key)
                                    .initiated(now)
                                    .build());
                })
                .doOnNext(upload -> log.debug("CreateMultipartUpload: bucket={}, key={}, uploadId={}",
                        bucketName, key, upload.getUploadId()));
    }

    @Override
    public Class<CreateMultipartUploadOperation> getOperationType() {
        return CreateMultipartUploadOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.WRITE, Capability.MULTIPART_UPLOAD);
    }
}
