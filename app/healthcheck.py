"""Container healthcheck entrypoint: ``python -m app.healthcheck``.

Loads config to discover the health file path, downstream probe port, and rate
thresholds, evaluates health, prints the reason, and exits 0 (healthy) or 1
(unhealthy). Any unexpected error also exits 1 so Docker treats it as unhealthy.
"""

from __future__ import annotations

import argparse
import sys

from app.config import ConfigManager
from app.health import evaluate_health


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="EM540 bridge healthcheck")
    parser.add_argument(
        "--config",
        default="/etc/carlo-gavazzi-em540-bridge/config.yaml",
        help="Path to configuration file",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    try:
        state = ConfigManager(args.config).load()
        health = state.health
        # The file must be no older than a couple of write intervals — long
        # enough to tolerate a missed write, short enough to catch a wedged
        # writer/tick loop well within the Docker healthcheck interval.
        max_file_age_s = max(3.0, health.write_interval * 3.0)
        result = evaluate_health(
            file_path=health.file,
            tcp_host="127.0.0.1",
            tcp_port=health.tcp_probe_port,
            rate_margin=health.rate_margin,
            unpaced_min_rate_hz=health.unpaced_min_rate_hz,
            max_file_age_s=max_file_age_s,
        )
    except Exception as exc:  # noqa: BLE001 - any failure means unhealthy
        print(f"healthcheck error: {exc}", file=sys.stderr)
        return 1

    stream = sys.stdout if result.healthy else sys.stderr
    print(result.reason, file=stream)
    return 0 if result.healthy else 1


if __name__ == "__main__":
    sys.exit(main())
