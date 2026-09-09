# Passive RS485 / Modbus-RTU bus sniffer — TEMPORARY DIAGNOSTIC.
# Standalone: not part of the main bridge image. Build/run separately.
FROM python:3.14-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Serial access requires membership of the dialout/tty groups (matches the
# main bridge image convention).
RUN groupadd -r sniffer && useradd -r -g sniffer -G dialout,tty -m -d /home/sniffer sniffer

# Only dependency is pyserial.
RUN pip install --no-cache-dir "pyserial>=3.5"

COPY sniffer.py ./sniffer.py

# Output directory for the CSV logs (bind-mount a host path here in compose).
RUN mkdir -p /data && chown -R sniffer:sniffer /app /data
VOLUME ["/data"]

USER sniffer

CMD ["python", "sniffer.py"]
