# Use an official Python runtime as a base image
FROM python:3.14-slim

# Set working directory
WORKDIR /app

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
RUN printf '"""Version of the EM540 bridge integration."""\n\n__version__ = "%s"\n' "$VERSION" > app/version.py

# Set permissions
RUN chown -R lerebel103:lerebel103 /app

# Switch to non-root user
USER lerebel103

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "app", "--config", "/etc/em540-bridge/config.yaml"]
