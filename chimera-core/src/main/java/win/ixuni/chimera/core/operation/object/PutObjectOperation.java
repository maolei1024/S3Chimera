package win.ixuni.chimera.core.operation.object;

import lombok.Builder;
import lombok.Value;
import reactor.core.publisher.Flux;
import win.ixuni.chimera.core.model.S3Object;
import win.ixuni.chimera.core.operation.Operation;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Put object operation
 */
@Value
@Builder
public class PutObjectOperation implements Operation<S3Object> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;

    /**
     * 对象内容流
     */
    Flux<ByteBuffer> content;

    /**
     * Content type
     */
    String contentType;

    /**
     * User metadata
     */
    Map<String, String> metadata;
}
