package win.ixuni.chimera.server.auth;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.auth.CredentialsProvider;
import win.ixuni.chimera.core.auth.S3Credentials;

/**
 * Configuration-file-based static credentials provider
 */
@Slf4j
@Component
@RequiredArgsConstructor
@EnableConfigurationProperties(AuthProperties.class)
public class StaticCredentialsProvider implements CredentialsProvider {

    private final AuthProperties authProperties;

    @Override
    public Mono<S3Credentials> getCredentials(String accessKeyId) {
        return Mono.justOrEmpty(
                authProperties.getCredentials().stream()
                        .filter(c -> c.getAccessKeyId().equals(accessKeyId))
                        .findFirst()
                        .map(c -> S3Credentials.builder()
                                .accessKeyId(c.getAccessKeyId())
                                .secretAccessKey(c.getSecretAccessKey())
                                .userId(c.getUserId())
                                .description(c.getDescription())
                                .enabled(c.isEnabled())
                                .build())
        );
    }
}
