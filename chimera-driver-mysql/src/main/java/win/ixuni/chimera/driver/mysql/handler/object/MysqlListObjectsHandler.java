package win.ixuni.chimera.driver.mysql.handler.object;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.model.ListObjectsResult;
import win.ixuni.chimera.core.operation.object.ListObjectsOperation;
import win.ixuni.chimera.core.operation.object.ListObjectsV2Operation;
import win.ixuni.chimera.driver.mysql.context.MysqlDriverContext;
import win.ixuni.chimera.driver.mysql.handler.AbstractMysqlHandler;
import win.ixuni.chimera.core.driver.DriverCapabilities.Capability;
import java.util.EnumSet;
import java.util.Set;

/**
 * MySQL 列出对象处理器 (V1)
 */
@Slf4j
public class MysqlListObjectsHandler extends AbstractMysqlHandler<ListObjectsOperation, ListObjectsResult> {

    @Override
    protected Mono<ListObjectsResult> doHandle(ListObjectsOperation operation, MysqlDriverContext ctx) {
        // V1 delegates to V2 implementation and converts result via context.execute()
        return ctx.execute(new ListObjectsV2Operation(operation.getRequest()))
                .map(v2 -> {
                    String nextMarker = null;
                    if (Boolean.TRUE.equals(v2.getIsTruncated())) {
                        if (v2.getContents() != null && !v2.getContents().isEmpty()) {
                            nextMarker = v2.getContents().get(v2.getContents().size() - 1).getKey();
                        } else if (v2.getCommonPrefixes() != null && !v2.getCommonPrefixes().isEmpty()) {
                            nextMarker = v2.getCommonPrefixes().get(v2.getCommonPrefixes().size() - 1).getPrefix();
                        }
                    }

                    return ListObjectsResult.builder()
                            .bucketName(v2.getName())
                            .prefix(v2.getPrefix())
                            .delimiter(v2.getDelimiter())
                            .isTruncated(v2.getIsTruncated())
                            .nextMarker(nextMarker)
                            .contents(v2.getContents())
                            .commonPrefixes(v2.getCommonPrefixes() != null ? v2.getCommonPrefixes().stream()
                                    .map(cp -> ListObjectsResult.CommonPrefix.builder()
                                            .prefix(cp.getPrefix()).build())
                                    .toList() : null)
                            .build();
                });
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
