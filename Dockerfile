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

# Create non-root user with home directory for security
RUN groupadd -r lerebel103 && useradd -r -g lerebel103 -G dialout,tty -m -d /home/lerebel103 lerebel103
ENV HOME=/home/lerebel103

# Install uv (pinned for reproducible builds)
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /bin/

# Copy dependency files for better Docker layer caching
COPY pyproject.toml uv.lock ./

# Install production dependencies only (no dev group), then remove build cache to
# avoid root-owned files in the non-root user's home and reduce image size
RUN uv sync --frozen --no-dev --no-install-project && rm -rf /home/lerebel103/.cache

# Disable uv cache at runtime (deps are baked in; no cache needed)
ENV UV_NO_CACHE=1

# Copy application source code
COPY app/ ./app/

# Inject version from build arg (set by Makefile / CI from git tag)
ARG VERSION=dev
ENV EM540_BRIDGE_VERSION=${VERSION}

# Set permissions
RUN chown -R lerebel103:lerebel103 /app

# Expose Modbus and emulation ports
EXPOSE 5001 5002 5003

# No HEALTHCHECK in the image: the freshness healthcheck is defined in
# docker-compose.yaml instead, so its thresholds can be tuned per deployment
# without rebuilding. It is a pure-shell probe over a heartbeat file the app
# writes on its diagnostics cadence (last successful upstream frame time), not a
# process-spawning functional check, so it does not perturb the 10Hz tick loop.

# Switch to non-root user
USER lerebel103

# Set Python path
ENV PYTHONPATH=/app

# Default command (--no-sync skips redundant env check since deps are baked in at build)
CMD ["uv", "run", "--frozen", "--no-sync", "python", "-m", "app", "--config", "/etc/carlo-gavazzi-em540-bridge/config.yaml"]
