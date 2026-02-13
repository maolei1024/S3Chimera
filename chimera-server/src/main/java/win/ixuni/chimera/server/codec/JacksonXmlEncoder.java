package win.ixuni.chimera.server.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.springframework.core.ResolvableType;
import org.springframework.core.codec.AbstractEncoder;
import org.springframework.core.codec.EncodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import java.util.Map;

/**
 * Custom Jackson XML encoder
 * Replaces deprecated Jackson2JsonEncoder (when handling XML)
 */
public class JacksonXmlEncoder extends AbstractEncoder<Object> {

    private final ObjectMapper mapper;

    public JacksonXmlEncoder(XmlMapper mapper, MimeType... mimeTypes) {
        super(mimeTypes);
        this.mapper = mapper;
    }

    @Override
    public boolean canEncode(ResolvableType elementType, @Nullable MimeType mimeType) {
        return super.canEncode(elementType, mimeType) && this.mapper.canSerialize(elementType.toClass());
    }

    @Override
    public reactor.core.publisher.Flux<DataBuffer> encode(org.reactivestreams.Publisher<?> inputStream,
            DataBufferFactory bufferFactory,
            ResolvableType elementType, @Nullable MimeType mimeType,
            @Nullable Map<String, Object> hints) {
        return reactor.core.publisher.Flux.from(inputStream)
                .map(value -> encodeValue(value, bufferFactory, elementType, mimeType, hints));
    }

    @Override
    public DataBuffer encodeValue(Object value, DataBufferFactory bufferFactory,
            ResolvableType valueType, @Nullable MimeType mimeType,
            @Nullable Map<String, Object> hints) {
        try {
            byte[] bytes = this.mapper.writeValueAsBytes(value);
            DataBuffer buffer = bufferFactory.allocateBuffer(bytes.length);
            buffer.write(bytes);
            return buffer;
        } catch (JsonProcessingException e) {
            throw new EncodingException("JSON encoding error: " + e.getMessage(), e);
        }
    }
}
