package win.ixuni.chimera.server.filter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.server.config.CorsProperties;

import java.util.List;

/**
 * CORS 过滤器
 * <p>
 * Handles cross-origin requests with all CORS headers required by the S3 API.
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
@RequiredArgsConstructor
@EnableConfigurationProperties(CorsProperties.class)
public class CorsFilter implements WebFilter {

    private final CorsProperties corsProperties;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!corsProperties.isEnabled()) {
            return chain.filter(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        ServerHttpResponse response = exchange.getResponse();

        String origin = request.getHeaders().getOrigin();
        
        // If no Origin header, not a cross-origin request
        if (origin == null) {
            return chain.filter(exchange);
        }

        // 检查是否允许该源
        if (!isOriginAllowed(origin)) {
            return chain.filter(exchange);
        }

        // Add CORS response headers
        HttpHeaders headers = response.getHeaders();
        
        // 允许的源
        if (corsProperties.getAllowedOrigins().contains("*")) {
            headers.set(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        } else {
            headers.set(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        }

        // 允许携带凭证
        if (corsProperties.isAllowCredentials()) {
            headers.set(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }

        // Exposed response headers
        if (!corsProperties.getExposedHeaders().isEmpty()) {
            headers.set(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS,
                    String.join(", ", corsProperties.getExposedHeaders()));
        }

        // Handle preflight requests (OPTIONS)
        if (request.getMethod() == HttpMethod.OPTIONS) {
            return handlePreflightRequest(exchange);
        }

        return chain.filter(exchange);
    }

    /**
     * Handle preflight request
     */
    private Mono<Void> handlePreflightRequest(ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        HttpHeaders headers = response.getHeaders();

        // 允许的方法
        headers.set(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS,
                String.join(", ", corsProperties.getAllowedMethods()));

        // 允许的请求头
        headers.set(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS,
                String.join(", ", corsProperties.getAllowedHeaders()));

        // Preflight request cache duration
        headers.set(HttpHeaders.ACCESS_CONTROL_MAX_AGE,
                String.valueOf(corsProperties.getMaxAge()));

        response.setStatusCode(HttpStatus.NO_CONTENT);
        return response.setComplete();
    }

    /**
     * 检查源是否被允许
     */
    private boolean isOriginAllowed(String origin) {
        List<String> allowedOrigins = corsProperties.getAllowedOrigins();
        
        // Allow all origins
        if (allowedOrigins.contains("*")) {
            return true;
        }

        // 精确匹配
        if (allowedOrigins.contains(origin)) {
            return true;
        }

        // 通配符匹配（如 *.example.com）
        for (String allowed : allowedOrigins) {
            if (allowed.startsWith("*.")) {
                String domain = allowed.substring(2);
                if (origin.endsWith("." + domain) || origin.equals("http://" + domain) || origin.equals("https://" + domain)) {
                    return true;
                }
            }
        }

        return false;
    }
}
