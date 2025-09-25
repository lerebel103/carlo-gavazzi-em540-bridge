import logging
import os
from schema import Use, And
from pyconfigparser import configparser, ConfigError

SCHEMA_CONFIG = {
    'em540_master': {
        'host': str,
        'port': And(Use(int), lambda n: 0 < n < 65535),
        'slave_id': And(Use(int), lambda n: 0 < n < 256),
        'timeout': And(Use(float), lambda n: 0 < n < 60),
        'retries': And(Use(int), lambda n: 0 <= n < 10),
        'update_interval': And(Use(float), lambda n: 0 < n < 10000),
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    },
    'em540_slave': {
        'host': str,
        'port': And(Use(int), lambda n: 0 < n < 65535),
        'slave_id': And(Use(int), lambda n: 0 < n < 256),
        'update_timeout': And(Use(float), lambda n: 0 < n < 60),
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    },
    'ts65a_slave': {
        'host': str,
        'port': And(Use(int), lambda n: 0 < n < 65535),
        'slave_id': And(Use(int), lambda n: 0 < n < 256),
        'update_timeout': And(Use(float), lambda n: 0 < n < 60),
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    },
    'mqtt': {
        'enabled': Use(bool),
        'host': str,
        'port': And(Use(int), lambda n: 0 < n < 65535),
        'username': str,
        'password': str,
        'update_interval': And(Use(float), lambda n: 0 < n < 10000),
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    },
    'pymodbus': {
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    },
    'root': {
        'logging': And(Use(str), lambda n: n in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    }
}


def load_config(config_file: str):
    config_dir = os.path.dirname(config_file)
    config_file = os.path.basename(config_file)
    print(f"Loading config from {os.path.join(config_dir, config_file)}")

    try:
        configparser.get_config(SCHEMA_CONFIG, config_dir=config_dir, file_name=config_file)
    except ConfigError as e:
        print(e)
        exit()