package win.ixuni.chimera.core.util;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * AWS S3 Chunked Transfer Encoding 解码器
 * <p>
 * When the AWS SDK uses chunkedEncodingEnabled(true), the request body format is:
 * 
 * <pre>
 * &lt;chunk-size-hex&gt;;chunk-signature=&lt;signature&gt;\r\n
 * &lt;chunk-data&gt;\r\n
 * ...
 * 0;chunk-signature=&lt;final-signature&gt;\r\n
 * \r\n
 * </pre>
 * <p>
 * 本解码器剥离 chunk headers 和 signatures，只返回实际数据。
 */
public class AwsChunkedDecoder {

    private static final String STREAMING_PREFIX = "STREAMING-";
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte SEMICOLON = ';';

    /**
     * Determine whether request uses AWS Chunked Encoding
     *
     * @param contentSha256 x-amz-content-sha256 header 值
     * @return true 如果是 AWS Chunked Encoding
     */
    public static boolean isAwsChunkedEncoding(String contentSha256) {
        return contentSha256 != null && contentSha256.startsWith(STREAMING_PREFIX);
    }

    /**
     * 解码 AWS Chunked Encoded 流
     * <p>
     * 将包含 chunk-signature 的原始流转换为纯数据流
     *
     * @param input 原始 DataBuffer 流
     * @return 解码后的 ByteBuffer 流
     */
    public static Flux<ByteBuffer> decode(Flux<DataBuffer> input) {
        return input
                // First collect all data (may need optimization for large files, but correctness first)
                .reduce(new ArrayList<byte[]>(), (list, dataBuffer) -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    list.add(bytes);
                    return list;
                })
                .flatMapMany(byteArrays -> {
                    // Merge all bytes
                    int totalLength = byteArrays.stream().mapToInt(b -> b.length).sum();
                    byte[] allBytes = new byte[totalLength];
                    int offset = 0;
                    for (byte[] arr : byteArrays) {
                        System.arraycopy(arr, 0, allBytes, offset, arr.length);
                        offset += arr.length;
                    }

                    // 解码 chunked 数据
                    List<ByteBuffer> decodedChunks = decodeChunkedData(allBytes);
                    return Flux.fromIterable(decodedChunks);
                });
    }

    /**
     * 解码 chunked 数据
     * 
     * @param data 原始 chunked 编码数据
     * @return 解码后的数据块列表
     */
    private static List<ByteBuffer> decodeChunkedData(byte[] data) {
        List<ByteBuffer> result = new ArrayList<>();
        int pos = 0;

        while (pos < data.length) {
            // 查找 chunk header 结束位置 (CRLF)
            int headerEnd = findCRLF(data, pos);
            if (headerEnd == -1) {
                break;
            }

            // 解析 chunk size (格式: <size-hex>;chunk-signature=...)
            String headerLine = new String(data, pos, headerEnd - pos, StandardCharsets.UTF_8);
            int chunkSize = parseChunkSize(headerLine);

            if (chunkSize == 0) {
                // Last chunk, done
                break;
            }

            // 跳过 CRLF
            int dataStart = headerEnd + 2;

            // 读取 chunk 数据
            if (dataStart + chunkSize <= data.length) {
                byte[] chunkData = new byte[chunkSize];
                System.arraycopy(data, dataStart, chunkData, 0, chunkSize);
                result.add(ByteBuffer.wrap(chunkData));
            }

            // Move to next chunk (data is followed by CRLF)
            pos = dataStart + chunkSize + 2;
        }

        return result;
    }

    /**
     * 解析 chunk size
     * 
     * @param headerLine chunk header 行，格式: &lt;size-hex&gt;;chunk-signature=...
     * @return chunk 大小
     */
    private static int parseChunkSize(String headerLine) {
        // 找到分号位置
        int semicolonPos = headerLine.indexOf(';');
        String sizeHex;
        if (semicolonPos > 0) {
            sizeHex = headerLine.substring(0, semicolonPos);
        } else {
            sizeHex = headerLine.trim();
        }

        try {
            return Integer.parseInt(sizeHex.trim(), 16);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 在数据中查找 CRLF 的位置
     * 
     * @param data   数据
     * @param offset 起始位置
     * @return CRLF 的起始位置，未找到返回 -1
     */
    private static int findCRLF(byte[] data, int offset) {
        for (int i = offset; i < data.length - 1; i++) {
            if (data[i] == '\r' && data[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }
}
