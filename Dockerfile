# syntax=docker/dockerfile:1
# =================================================================
# 🐳 S3Chimera Dockerfile - Multi-stage Build
# =================================================================

# --- Stage 1: Build ---
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

# 复制 Gradle 包装器和配置文件
COPY gradlew gradlew.bat settings.gradle build.gradle ./
COPY gradle/ gradle/

# Copy build.gradle from all modules
COPY chimera-core/build.gradle chimera-core/
COPY chimera-server/build.gradle chimera-server/
COPY chimera-driver-memory/build.gradle chimera-driver-memory/
COPY chimera-driver-mysql/build.gradle chimera-driver-mysql/
COPY chimera-driver-local/build.gradle chimera-driver-local/
COPY chimera-driver-s3/build.gradle chimera-driver-s3/
COPY chimera-driver-webdav/build.gradle chimera-driver-webdav/
COPY chimera-driver-sftp/build.gradle chimera-driver-sftp/
COPY chimera-driver-mongodb/build.gradle chimera-driver-mongodb/
COPY chimera-driver-postgresql/build.gradle chimera-driver-postgresql/

# 创建测试模块空目录 (Gradle 配置阶段需要)
RUN mkdir -p chimera-test-s3

# Download dependencies (with persistent Gradle cache)
RUN --mount=type=cache,target=/root/.gradle/caches \
    --mount=type=cache,target=/root/.gradle/wrapper \
    chmod +x gradlew && ./gradlew dependencies --no-daemon || true

# 复制源代码
COPY chimera-core/src/ chimera-core/src/
COPY chimera-server/src/ chimera-server/src/
COPY chimera-driver-memory/src/ chimera-driver-memory/src/
COPY chimera-driver-mysql/src/ chimera-driver-mysql/src/
COPY chimera-driver-local/src/ chimera-driver-local/src/
COPY chimera-driver-s3/src/ chimera-driver-s3/src/
COPY chimera-driver-webdav/src/ chimera-driver-webdav/src/
COPY chimera-driver-sftp/src/ chimera-driver-sftp/src/
COPY chimera-driver-mongodb/src/ chimera-driver-mongodb/src/
COPY chimera-driver-postgresql/src/ chimera-driver-postgresql/src/

# Build executable JAR (skip tests)
RUN --mount=type=cache,target=/root/.gradle/caches \
    --mount=type=cache,target=/root/.gradle/wrapper \
    ./gradlew :chimera-server:bootJar --no-daemon -x test

# --- 阶段二：运行 ---
FROM eclipse-temurin:21-jre

LABEL maintainer="ixuni" \
    description="S3Chimera - S3 Compatible Storage Gateway" \
    version="0.0.1-SNAPSHOT"

WORKDIR /app

# Create non-root user
RUN groupadd -r chimera && useradd -r -g chimera chimera

# Create config directory (for mounting external configs)
RUN mkdir -p /app/config && chown -R chimera:chimera /app/config

# 从构建阶段复制 JAR
COPY --from=builder /app/chimera-server/build/libs/*.jar app.jar

# 设置文件权限
RUN chown -R chimera:chimera /app

USER chimera

# Expose port (default S3Chimera port)
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9000/actuator/health || exit 1

# =================================================================
# 配置说明:
# -----------------------------------------------------------------
# 方式1: 环境变量 (推荐)
#   docker run -e CHIMERA_DRIVERS_0_NAME=mysql-main \
#              -e CHIMERA_DRIVERS_0_TYPE=sql \
#              -e CHIMERA_DRIVERS_0_ENABLED=true \
#              -e CHIMERA_DRIVERS_0_PROPERTIES_URL=r2dbc:mysql://host:3306/db \
#              -e CHIMERA_DRIVERS_0_PROPERTIES_USERNAME=root \
#              -e CHIMERA_DRIVERS_0_PROPERTIES_PASSWORD=secret \
#              s3chimera
#
# 方式2: 挂载配置文件
#   docker run -v /path/to/application.yml:/app/config/application.yml s3chimera
#
# 方式3: 命令行参数
#   docker run s3chimera --chimera.routing.default-driver=mysql-main
# =================================================================

# JVM optimization parameters
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+UseZGC"

# Spring Boot 外部配置目录（完全覆盖模式）
# 先加载 JAR 内配置，再加载外部配置，外部配置会覆盖 JAR 内配置
ENV SPRING_CONFIG_LOCATION="optional:classpath:/,optional:file:/app/config/"

# 启动命令
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar $0 $@"]
