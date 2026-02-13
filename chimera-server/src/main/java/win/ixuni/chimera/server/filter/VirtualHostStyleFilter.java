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
import java.util.regex.Pattern;

/**
 * Virtual-Host-Style 请求转换过滤器
 * <p>
 * 将 virtual-host-style 请求 (bucket.endpoint.com/key)
 * 转换为 path-style 请求 (endpoint.com/bucket/key)
 * <p>
 * 示例:
 * <ul>
 *   <li>mybucket.s3.example.com/photo.jpg → s3.example.com/mybucket/photo.jpg</li>
 *   <li>mybucket.localhost:9000/photo.jpg → localhost:9000/mybucket/photo.jpg</li>
 * </ul>
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class VirtualHostStyleFilter implements WebFilter {

    private final ChimeraProperties chimeraProperties;

    /**
     * Configured base domain (from config or auto-detected)
     * 如果 Host 为 bucket.base-domain，则提取 bucket
     */
    private static final Pattern IP_PATTERN = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // If virtual-host-style is not enabled, pass through
        if (!chimeraProperties.getServer().isVirtualHostStyle()) {
            return chain.filter(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        String host = request.getHeaders().getHost() != null
                ? request.getHeaders().getHost().getHostName()
                : null;

        if (host == null) {
            return chain.filter(exchange);
        }

        // 检查是否是 virtual-host-style 请求
        String bucket = extractBucketFromHost(host);
        if (bucket == null) {
            // 不是 virtual-host-style，直接放行
            return chain.filter(exchange);
        }

        // 重写请求路径
        String originalPath = request.getURI().getRawPath();
        String newPath = "/" + bucket + (originalPath.isEmpty() ? "/" : originalPath);

        log.debug("Virtual-host-style: {} {} -> {}", host, originalPath, newPath);

        // 创建新的请求
        ServerHttpRequest newRequest = request.mutate()
                .uri(buildNewUri(request.getURI(), newPath))
                .path(newPath)
                .build();

        return chain.filter(exchange.mutate().request(newRequest).build());
    }

    /**
     * 从 Host 头提取 bucket 名称
     * <p>
     * 规则：
     * <ul>
     *   <li>如果配置了 base-domain，检查 Host 是否为 bucket.base-domain</li>
     *   <li>Otherwise, check if Host has at least two subdomain levels</li>
     *   <li>IP 地址不进行提取</li>
     * </ul>
     *
     * @param host Host 头值（不含端口）
     * @return bucket 名称，null 表示不是 virtual-host-style
     */
    private String extractBucketFromHost(String host) {
        // IP 地址不支持 virtual-host-style
        if (IP_PATTERN.matcher(host).matches()) {
            return null;
        }

        // localhost 特殊处理
        if (host.equals("localhost") || host.equals("127.0.0.1")) {
            return null;
        }

        String baseDomain = chimeraProperties.getServer().getBaseDomain();

        if (baseDomain != null && !baseDomain.isEmpty()) {
            // 配置了 base-domain，检查是否匹配
            if (host.endsWith("." + baseDomain)) {
                String bucket = host.substring(0, host.length() - baseDomain.length() - 1);
                if (isValidBucketName(bucket)) {
                    return bucket;
                }
            }
            return null;
        }

        // base-domain not configured, check for subdomains
        // 例如: mybucket.localhost -> 提取 mybucket
        // 例如: mybucket.s3.example.com -> 提取 mybucket
        int firstDot = host.indexOf('.');
        if (firstDot > 0) {
            String potentialBucket = host.substring(0, firstDot);
            String remainingHost = host.substring(firstDot + 1);

            // Ensure remaining part is not empty and bucket name is valid
            if (!remainingHost.isEmpty() && isValidBucketName(potentialBucket)) {
                return potentialBucket;
            }
        }

        return null;
    }

    /**
     * Check whether bucket name is valid
     * S3 bucket 名称规则：3-63 字符，小写字母、数字、连字符
     */
    private boolean isValidBucketName(String name) {
        if (name == null || name.length() < 3 || name.length() > 63) {
            return false;
        }
        // 简化验证：只检查基本字符
        return name.matches("^[a-z0-9][a-z0-9.-]*[a-z0-9]$");
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
