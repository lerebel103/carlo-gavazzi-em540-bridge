#!/bin/sh
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running victron's dbus-cgwacs over virtual tty created for modbus/rtu over sockets"
/opt/victronenergy/dbus-cgwacs/dbus-cgwacs /dev/ttyV0
