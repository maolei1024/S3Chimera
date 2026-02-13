package win.ixuni.chimera.driver.sftp.handler.object;

import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.DriverContext;
import win.ixuni.chimera.core.operation.OperationHandler;
import win.ixuni.chimera.core.operation.object.CopyObjectOperation;
import win.ixuni.chimera.core.operation.object.GetObjectOperation;
import win.ixuni.chimera.core.operation.object.PutObjectOperation;
import win.ixuni.chimera.driver.sftp.context.SftpDriverContext;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * SFTP 复制对象处理器
 * <p>
 * Implemented via GET + PUT, proxying operations through the handler registry.
 */
public class SftpCopyObjectHandler implements OperationHandler<CopyObjectOperation, S3Object> {

    @Override
    public Mono<S3Object> handle(CopyObjectOperation operation, DriverContext context) {
        SftpDriverContext ctx = (SftpDriverContext) context;

        // 通过 GET + PUT 实现复制
        GetObjectOperation getOp = new GetObjectOperation(
                operation.getSourceBucket(),
                operation.getSourceKey());

        return ctx.getHandlerRegistry().execute(getOp, context)
                .flatMap(sourceData -> {
                    S3Object meta = sourceData.getMetadata();
                    PutObjectOperation putOp = PutObjectOperation.builder()
                            .bucketName(operation.getDestinationBucket())
                            .key(operation.getDestinationKey())
                            .content(sourceData.getContent())
                            .contentType(meta.getContentType())
                            .metadata(meta.getUserMetadata() != null ? meta.getUserMetadata() : Map.of())
                            .build();
                    return ctx.getHandlerRegistry().execute(putOp, context);
                });
    }

    @Override
    public Class<CopyObjectOperation> getOperationType() {
        return CopyObjectOperation.class;
    }

    @Override
    public Set<Capability> getProvidedCapabilities() {
        return EnumSet.of(Capability.COPY);
    }
}
