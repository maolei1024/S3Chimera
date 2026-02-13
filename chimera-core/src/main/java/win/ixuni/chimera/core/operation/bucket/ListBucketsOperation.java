package win.ixuni.chimera.core.operation.bucket;

import lombok.Value;
import reactor.core.publisher.Flux;
import win.ixuni.chimera.core.model.S3Bucket;
import win.ixuni.chimera.core.operation.Operation;

/**
 * List all buckets operation
 */
@Value
public class ListBucketsOperation implements Operation<Flux<S3Bucket>> {
    // 无参数
}
