package win.ixuni.chimera.core.operation.multipart;

import lombok.Value;
import reactor.core.publisher.Flux;
import win.ixuni.chimera.core.model.UploadPart;
import win.ixuni.chimera.core.operation.Operation;

import java.nio.ByteBuffer;

/**
 * 上传分片操作
 */
@Value
public class UploadPartOperation implements Operation<UploadPart> {

    /**
     * Bucket 名称
     */
    String bucketName;

    /**
     * 对象 Key
     */
    String key;

    /**
     * 上传 ID
     */
    String uploadId;

    /**
     * 分片编号 (1-10000)
     */
    Integer partNumber;

    /**
     * 分片内容
     */
    Flux<ByteBuffer> content;
}
