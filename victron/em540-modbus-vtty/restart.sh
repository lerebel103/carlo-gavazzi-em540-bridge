#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# This is not right, hammer approach with killing the dbus-cgwacs process
killall -9 socat