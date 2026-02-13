package win.ixuni.chimera.server.filter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.config.ChimeraProperties;

import java.net.URI;

/**
 * Path Prefix 过滤器
 * <p>
 * Removes the configured path-prefix from the URL so requests route correctly to the controller.
 * <p>
 * 示例（配置 path-prefix: "/s3"）:
 * <ul>
 *   <li>/s3/my-bucket/photo.jpg → /my-bucket/photo.jpg</li>
 *   <li>/s3/my-bucket → /my-bucket</li>
 *   <li>/s3/ → /</li>
 * </ul>
 * <p>
 * Applicable scenarios:
 * <ul>
 *   <li>Reverse proxy routes S3 requests to the /s3 path</li>
 *   <li>Path isolation required when coexisting with other services</li>
 * </ul>
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1) // 在 VirtualHostStyleFilter 之后执行
@RequiredArgsConstructor
public class PathPrefixFilter implements WebFilter {

    private final ChimeraProperties chimeraProperties;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String pathPrefix = chimeraProperties.getServer().getPathPrefix();
        
        // If path-prefix is not configured or empty, pass through
        if (pathPrefix == null || pathPrefix.isEmpty()) {
            return chain.filter(exchange);
        }

        // Ensure prefix starts with / but does not end with /
        String normalizedPrefix = normalizePrefix(pathPrefix);
        if (normalizedPrefix.isEmpty()) {
            return chain.filter(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        String originalPath = request.getURI().getRawPath();

        // Check if path starts with configured prefix
        if (!originalPath.startsWith(normalizedPrefix)) {
            // 路径不匹配前缀，可能是非 S3 请求或配置错误
            log.debug("Path '{}' does not start with prefix '{}'", originalPath, normalizedPrefix);
            return chain.filter(exchange);
        }

        // Remove prefix
        String newPath = originalPath.substring(normalizedPrefix.length());
        
        // Ensure new path starts with /
        if (!newPath.startsWith("/")) {
            newPath = "/" + newPath;
        }

        log.debug("PathPrefix: {} -> {} (prefix: {})", originalPath, newPath, normalizedPrefix);

        // 创建新的请求
        ServerHttpRequest newRequest = request.mutate()
                .uri(buildNewUri(request.getURI(), newPath))
                .path(newPath)
                .build();

        return chain.filter(exchange.mutate().request(newRequest).build());
    }

    /**
     * Normalize prefix: ensure starts with / and does not end with /
     */
    private String normalizePrefix(String prefix) {
        String normalized = prefix.trim();
        
        // Add leading /
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        
        // Remove trailing /
        while (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        
        // If only / remains, return empty
        if (normalized.equals("/")) {
            return "";
        }
        
        return normalized;
    }

    /**
     * 构建新的 URI
     */
    private URI buildNewUri(URI original, String newPath) {
        try {
            return new URI(
                    original.getScheme(),
                    original.getUserInfo(),
                    original.getHost(),
                    original.getPort(),
                    newPath,
                    original.getRawQuery(),
                    original.getFragment()
            );
        } catch (Exception e) {
            log.error("Failed to build new URI: {}", e.getMessage());
            return original;
        }
    }
}
