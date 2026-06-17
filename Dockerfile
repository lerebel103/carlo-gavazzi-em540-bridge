# Use an official Python runtime as a base image
FROM python:3.14-slim

# Set working directory
WORKDIR /app

# Metadata labels for Docker Hub
LABEL org.opencontainers.image.title="Carlo Gavazzi EM540 Energy Meter Modbus Bridge"
LABEL org.opencontainers.image.description="Bridges a Carlo Gavazzi EM540/EM530 meter to Modbus, MQTT Home Assistant, and Fronius TS-65-A emulation"
LABEL org.opencontainers.image.url="https://github.com/lerebel103/carlo-gavazzi-em540-bridge"
LABEL org.opencontainers.image.source="https://github.com/lerebel103/carlo-gavazzi-em540-bridge"
LABEL org.opencontainers.image.documentation="https://github.com/lerebel103/carlo-gavazzi-em540-bridge#readme"
LABEL org.opencontainers.image.vendor="lerebel103"
LABEL org.opencontainers.image.licenses="MIT"

# Set Python to unbuffered mode for real-time logging in containers
ENV PYTHONUNBUFFERED=1

# Disable .pyc bytecode writing to avoid I/O delays in real-time tick loop
ENV PYTHONDONTWRITEBYTECODE=1

# Create non-root user for security
RUN groupadd -r lerebel103 && useradd -r -g lerebel103 lerebel103

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app/ ./app/

# Inject version from build arg (set by Makefile / CI from git tag)
ARG VERSION=dev
ENV EM540_BRIDGE_VERSION=${VERSION}

# Set permissions
RUN chown -R lerebel103:lerebel103 /app

# Expose Modbus and emulation ports
EXPOSE 5001 5002 5003

# Define healthcheck: ensure app process is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ps aux | grep -v grep | grep -q 'python -m app' || exit 1

# Switch to non-root user
USER lerebel103

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "app", "--config", "/etc/carlo-gavazzi-em540-bridge/config.yaml"]
