package win.ixuni.chimera.driver.local.handler.object;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.exception.ObjectNotFoundException;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.model.S3ObjectData;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.core.util.SidecarMetadata;
import win.ixuni.chimera.driver.local.context.LocalDriverContext;
import win.ixuni.chimera.driver.local.handler.LocalFileUtils;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

/**
 * 本地文件系统获取对象处理器
 */
public class LocalGetObjectHandler implements OperationHandler<GetObjectOperation, S3ObjectData> {

    @Override
    public Mono<S3ObjectData> handle(GetObjectOperation operation, DriverContext context) {
        LocalDriverContext ctx = (LocalDriverContext) context;
        String bucketName = operation.getBucketName();
        String key = operation.getKey();

        return Mono.fromCallable(() -> {
            var objectPath = ctx.getObjectPath(bucketName, key);
            var metaPath = ctx.getMetadataPath(bucketName, key);

            if (!Files.exists(objectPath)) {
                throw new ObjectNotFoundException(bucketName, key);
            }

            // 读取 Sidecar 元数据（带 fallback）
            SidecarMetadata sidecar = LocalFileUtils.readSidecar(objectPath, metaPath);

            // 读取文件内容
            byte[] data = Files.readAllBytes(objectPath);

            S3Object s3Object = S3Object.builder()
                    .bucketName(bucketName)
                    .key(key)
                    .size(sidecar.getSize())
                    .etag(sidecar.getEtag())
                    .lastModified(sidecar.getLastModifiedInstant())
                    .contentType(sidecar.getContentType())
                    .userMetadata(sidecar.getUserMetadata())
                    .build();

            return S3ObjectData.builder()
                    .metadata(s3Object)
                    .content(Flux.just(ByteBuffer.wrap(data)))
                    .build();
        });
    }

    @Override
    public Class<GetObjectOperation> getOperationType() {
        return GetObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.READ);
    }
}
