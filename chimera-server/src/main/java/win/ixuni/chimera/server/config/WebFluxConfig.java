package win.ixuni.chimera.server.config;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import win.ixuni.chimera.server.codec.JacksonXmlDecoder;
import win.ixuni.chimera.server.codec.JacksonXmlEncoder;

import java.time.Instant;

/**
 * WebFlux 配置
 * Configure XML serialization to support S3 API responses
 */
@Configuration
public class WebFluxConfig implements WebFluxConfigurer {

    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {
        XmlMapper xmlMapper = createXmlMapper();

        // Add custom Jackson XML encoder/decoder
        configurer.customCodecs().register(
                new JacksonXmlEncoder(xmlMapper, MediaType.APPLICATION_XML));
        configurer.customCodecs().register(
                new JacksonXmlDecoder(xmlMapper, MediaType.APPLICATION_XML));
    }

    @Bean
    @Primary
    public XmlMapper xmlMapper() {
        return createXmlMapper();
    }

    private XmlMapper createXmlMapper() {
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
        xmlMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        // Configure Java 8 time module, using S3-compatible date formats
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        // S3 uses ISO 8601 format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
        javaTimeModule.addSerializer(Instant.class, InstantSerializer.INSTANCE);
        xmlMapper.registerModule(javaTimeModule);

        return xmlMapper;
    }
}
