package win.ixuni.chimera.driver.webdav.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.BucketNotFoundException;
import win.ixuni.chimera.core.model.DeleteObjectsRequest;
import win.ixuni.chimera.core.model.DeleteObjectsResult;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.DeleteObjectsOperation;
import win.ixuni.chimera.driver.webdav.context.WebDavDriverContext;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * WebDAV 批量删除对象处理器
 */
public class WebDavDeleteObjectsHandler implements OperationHandler<DeleteObjectsOperation, DeleteObjectsResult> {

    @Override
    public Mono<DeleteObjectsResult> handle(DeleteObjectsOperation operation, DriverContext context) {
        WebDavDriverContext ctx = (WebDavDriverContext) context;
        DeleteObjectsRequest request = operation.getRequest();
        String bucketName = request.getBucketName();

        return Mono.fromCallable(() -> {
            String bucketUrl = ctx.getBucketUrl(bucketName);

            // 检查 bucket 是否存在
            if (!ctx.getSardine().exists(bucketUrl)) {
                throw new BucketNotFoundException(bucketName);
            }

            List<DeleteObjectsResult.DeletedObject> deleted = new ArrayList<>();
            List<DeleteObjectsResult.DeleteError> errors = new ArrayList<>();

            for (String key : request.getKeys()) {
                try {
                    String objectUrl = ctx.getObjectUrl(bucketName, key);
                    if (ctx.getSardine().exists(objectUrl)) {
                        ctx.getSardine().delete(objectUrl);
                    }

                    // 同时删除 Sidecar 元数据文件
                    try {
                        String metadataUrl = ctx.getMetadataUrl(bucketName, key);
                        if (ctx.getSardine().exists(metadataUrl)) {
                            ctx.getSardine().delete(metadataUrl);
                        }
                    } catch (Exception ignored) {
                        // Sidecar delete failure does not affect the main flow
                    }

                    // S3 semantics: report delete as successful even if the object does not exist
                    deleted.add(DeleteObjectsResult.DeletedObject.builder().key(key).build());
                } catch (Exception e) {
                    errors.add(DeleteObjectsResult.DeleteError.builder()
                            .key(key)
                            .code("InternalError")
                            .message(e.getMessage())
                            .build());
                }
            }

            return DeleteObjectsResult.builder()
                    .deleted(deleted)
                    .errors(errors.isEmpty() ? null : errors)
                    .build();
        });
    }

    @Override
    public Class<DeleteObjectsOperation> getOperationType() {
        return DeleteObjectsOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.BATCH_DELETE);
    }
}
