import os

from pyconfigparser import ConfigError, configparser
from schema import And, Use

SCHEMA_CONFIG = {
    "em540_master": {
        "mode": And(Use(str), lambda n: n in ["tcp", "serial"]),
        # If serial is selected, the following optional parameters are used
        "baudrate": And(Use(int), lambda n: n in [9600, 19200, 38400, 57600, 115200]),
        "parity": And(Use(str), lambda n: n in ["N", "E", "O"]),
        "stopbits": And(Use(int), lambda n: n in [1, 2]),
        "bytesize": And(Use(int), lambda n: n in [7, 8]),
        "serial_port": str,
        # End of serial parameters
        # If tcp is selected, the following parameters are used
        "host": str,
        "port": And(Use(int), lambda n: 0 < n < 65535),
        # End of tcp parameters
        "slave_id": And(Use(int), lambda n: 0 < n < 256),
        "timeout": And(Use(float), lambda n: 0 < n < 60),
        "retries": And(Use(int), lambda n: 0 <= n < 10),
        "update_interval": And(Use(float), lambda n: 0 < n < 10000),
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
    "em540_slave": {
        "host": str,
        "rtu_port": And(Use(int), lambda n: 0 < n < 65535),
        "tcp_port": And(Use(int), lambda n: 0 < n < 65535),
        "slave_id": And(Use(int), lambda n: 0 < n < 256),
        "update_timeout": And(Use(float), lambda n: 0 < n < 60),
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
    "ts65a_slave": {
        "host": str,
        "port": And(Use(int), lambda n: 0 < n < 65535),
        "slave_id": And(Use(int), lambda n: 0 < n < 256),
        "update_timeout": And(Use(float), lambda n: 0 < n < 60),
        "grid_feed_in_hard_limit": And(Use(float), lambda n: n <= 0),
        "smoothing_num_points": And(Use(int), lambda n: 1 <= n <= 50),
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
    "mqtt": {
        "enabled": Use(bool),
        "host": str,
        "port": And(Use(int), lambda n: 0 < n < 65535),
        "username": str,
        "password": str,
        "update_interval": And(Use(float), lambda n: 0 < n < 10000),
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
    "pymodbus": {
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
    "root": {
        "log_level": And(
            Use(str), lambda n: n in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        ),
    },
}


def load_config(config_file: str):
    config_dir = os.path.dirname(config_file)
    config_file = os.path.basename(config_file)
    print(f"Loading config from {os.path.join(config_dir, config_file)}")

    try:
        configparser.get_config(
            SCHEMA_CONFIG, config_dir=config_dir, file_name=config_file
        )
    except ConfigError as e:
        print(e)
        exit()
