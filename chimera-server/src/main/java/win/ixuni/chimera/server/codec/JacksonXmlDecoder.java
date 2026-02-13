package win.ixuni.chimera.server.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.springframework.core.ResolvableType;
import org.springframework.core.codec.AbstractDecoder;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Custom Jackson XML decoder
 * Replaces deprecated Jackson2JsonDecoder (when handling XML)
 */
public class JacksonXmlDecoder extends AbstractDecoder<Object> {

    private final ObjectMapper mapper;

    public JacksonXmlDecoder(XmlMapper mapper, MimeType... mimeTypes) {
        super(mimeTypes);
        this.mapper = mapper;
    }

    @Override
    public boolean canDecode(ResolvableType elementType, @Nullable MimeType mimeType) {
        return super.canDecode(elementType, mimeType)
                && this.mapper.canDeserialize(this.mapper.constructType(elementType.getType()));
    }

    @Override
    public Flux<Object> decode(org.reactivestreams.Publisher<DataBuffer> inputStream, ResolvableType elementType,
            @Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {
        // For XML, typically aggregate all buffers then parse
        return DataBufferUtils.join(inputStream)
                .flatMap(dataBuffer -> {
                    try {
                        InputStream is = dataBuffer.asInputStream();
                        Object value = this.mapper.readValue(is, this.mapper.constructType(elementType.getType()));
                        return Mono.justOrEmpty(value);
                    } catch (IOException e) {
                        return Mono.error(new DecodingException("JSON decoding error: " + e.getMessage(), e));
                    } finally {
                        DataBufferUtils.release(dataBuffer);
                    }
                })
                .flux();
    }

    @Override
    public Mono<Object> decodeToMono(org.reactivestreams.Publisher<DataBuffer> inputStream, ResolvableType elementType,
            @Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {
        return DataBufferUtils.join(inputStream)
                .flatMap(dataBuffer -> {
                    try {
                        InputStream is = dataBuffer.asInputStream();
                        Object value = this.mapper.readValue(is, this.mapper.constructType(elementType.getType()));
                        return Mono.justOrEmpty(value);
                    } catch (IOException e) {
                        return Mono.error(new DecodingException("JSON decoding error: " + e.getMessage(), e));
                    } finally {
                        DataBufferUtils.release(dataBuffer);
                    }
                });
    }
}
