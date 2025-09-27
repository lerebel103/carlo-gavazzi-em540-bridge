#!/bin/sh
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running modbus/rtu connection to virtual terminal"
/usr/bin/socat pty,link=/dev/ttyV0,raw tcp:192.168.102.240:5002
