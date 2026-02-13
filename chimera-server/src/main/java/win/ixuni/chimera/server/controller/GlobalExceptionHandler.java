package win.ixuni.chimera.server.controller;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import reactor.core.publisher.Mono;
import win.ixuni.chimera.core.exception.ChimeraException;

import java.util.UUID;

/**
 * Global exception handler
 * <p>
 * Converts exceptions to S3-standard error response format.
 * 
 * S3 error response format:
 * 
 * <pre>
 * &lt;Error&gt;
 *     &lt;Code&gt;NoSuchBucket&lt;/Code&gt;
 *     &lt;Message&gt;The specified bucket does not exist&lt;/Message&gt;
 *     &lt;Resource&gt;/mybucket&lt;/Resource&gt;
 *     &lt;RequestId&gt;xxx&lt;/RequestId&gt;
 * &lt;/Error&gt;
 * </pre>
 * 
 * Note: exclude actuator package to avoid interfering with health check endpoints
 */
@Slf4j
@RestControllerAdvice(basePackages = "win.ixuni.chimera")
public class GlobalExceptionHandler {

        private static final MediaType APPLICATION_XML = MediaType.valueOf("application/xml");

        @ExceptionHandler(ChimeraException.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleChimeraException(
                        ChimeraException ex, ServerHttpRequest request) {
                log.warn("S3 Error: {} - {}", ex.getErrorCode(), ex.getMessage());

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code(ex.getErrorCode())
                                .message(ex.getMessage())
                                .resource(request.getPath().value())
                                .requestId(generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(ex.getHttpStatus())
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        @ExceptionHandler(IllegalArgumentException.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleIllegalArgumentException(
                        IllegalArgumentException ex, ServerHttpRequest request) {
                log.warn("Invalid argument: {}", ex.getMessage());

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code("InvalidArgument")
                                .message(ex.getMessage())
                                .resource(request.getPath().value())
                                .requestId(generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(HttpStatus.BAD_REQUEST)
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        @ExceptionHandler(UnsupportedOperationException.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleUnsupportedOperationException(
                        UnsupportedOperationException ex, ServerHttpRequest request) {
                log.warn("Unsupported operation: {}", ex.getMessage());

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code("NotImplemented")
                                .message(ex.getMessage() != null ? ex.getMessage()
                                                : "The requested feature is not supported by this driver")
                                .resource(request.getPath().value())
                                .requestId(generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(HttpStatus.NOT_IMPLEMENTED)
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        /**
         * Handle AWS SDK S3 exceptions (thrown by S3 Proxy driver)
         * Convert AWS SDK exceptions to S3-standard error responses
         */
        @ExceptionHandler(software.amazon.awssdk.services.s3.model.S3Exception.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleAwsS3Exception(
                        software.amazon.awssdk.services.s3.model.S3Exception ex, ServerHttpRequest request) {
                log.warn("AWS S3 Error: {} - {}", ex.awsErrorDetails().errorCode(), ex.getMessage());

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code(ex.awsErrorDetails().errorCode())
                                .message(ex.awsErrorDetails().errorMessage())
                                .resource(request.getPath().value())
                                .requestId(ex.requestId() != null ? ex.requestId() : generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(ex.statusCode())
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        /**
         * Handle Sardine exceptions (thrown by WebDAV driver)
         * Convert WebDAV HTTP errors to S3-standard error responses
         */
        @ExceptionHandler(com.github.sardine.impl.SardineException.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleSardineException(
                        com.github.sardine.impl.SardineException ex, ServerHttpRequest request) {
                log.warn("WebDAV Error: {} - {}", ex.getStatusCode(), ex.getMessage());

                // Map WebDAV HTTP status codes to S3 error codes
                String errorCode;
                int statusCode = ex.getStatusCode();
                switch (statusCode) {
                        case 404 -> errorCode = "NoSuchKey";
                        case 403 -> errorCode = "AccessDenied";
                        case 409 -> errorCode = "BucketAlreadyExists";
                        default -> errorCode = "InternalError";
                }

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code(errorCode)
                                .message(ex.getResponsePhrase() != null ? ex.getResponsePhrase() : ex.getMessage())
                                .resource(request.getPath().value())
                                .requestId(generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(statusCode)
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        @ExceptionHandler(Exception.class)
        public Mono<ResponseEntity<S3ErrorResponse>> handleGenericException(
                        Exception ex, ServerHttpRequest request) {
                log.error("Internal error: {}", ex.getMessage(), ex);

                S3ErrorResponse error = S3ErrorResponse.builder()
                                .code("InternalError")
                                .message("An internal error occurred")
                                .resource(request.getPath().value())
                                .requestId(generateRequestId())
                                .build();

                return Mono.just(ResponseEntity
                                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                                .contentType(APPLICATION_XML)
                                .body(error));
        }

        private String generateRequestId() {
                return UUID.randomUUID().toString().replace("-", "").toUpperCase().substring(0, 16);
        }

        /**
         * S3-standard error response
         */
        @lombok.Data
        @lombok.Builder
        @lombok.NoArgsConstructor
        @lombok.AllArgsConstructor
        @JacksonXmlRootElement(localName = "Error")
        public static class S3ErrorResponse {
                @JacksonXmlProperty(localName = "Code")
                private String code;

                @JacksonXmlProperty(localName = "Message")
                private String message;

                @JacksonXmlProperty(localName = "Resource")
                private String resource;

                @JacksonXmlProperty(localName = "RequestId")
                private String requestId;

                @JacksonXmlProperty(localName = "HostId")
                private String hostId;
        }
}
