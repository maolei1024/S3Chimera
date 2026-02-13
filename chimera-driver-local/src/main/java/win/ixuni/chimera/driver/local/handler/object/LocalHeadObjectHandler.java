package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.HeadObjectOperation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统获取对象元数据处理器
 */
public class LocalHeadObjectHandler implements OperationHandler<HeadObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(HeadObjectOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            var objectPath = ctx.getObjectPath(bucketName, key);
            var metaPath = ctx.getMetadataPath(bucketName, key);

            if (!Files.exists(objectPath)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            SidecarMetadata sidecar = LocalFileUtils.readSidecar(objectPath, metaPath);

            return S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(sidecar.getSize())
                    .etag(sidecar.getEtag())
                    .lastModified(sidecar.getLastModifiedInstant())
                    .contentType(sidecar.getContentType())
                    .userMetadata(sidecar.getUserMetadata())
                    .build();
        });
    }

    @Override
    public Class<HeadObjectOperation> getOperationType() {
        return HeadObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
