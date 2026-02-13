package win.ixuni.chimera.core.model;

import lombok.Builder;
import lombok.Data;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;

/**
 * S3 Object 数据（包含元数据和内容流）
 */
@Data
@Builder
public class S3ObjectData {
    
    /**
     * 对象元数据
     */
    private S3Object metadata;
    
    /**
     * Object content stream (reactive)
     */
    private Flux<ByteBuffer> content;
}
