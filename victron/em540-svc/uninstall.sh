#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SERVICE_NAME=$(basename $SCRIPT_DIR)

if  [ -e /service/$SERVICE_NAME ]
then
    rm /service/$SERVICE_NAME
    kill $(pgrep -f '$SERVICE_NAME.sh')
    chmod a-x $SCRIPT_DIR/service/run
    kill $(pgrep -f '$SERVICE_NAME.sh')  /dev/null 2> /dev/null
fi

# Remove install-script
grep -v "$SCRIPT_DIR/install.sh" /data/rc.local >> /data/temp.local
mv /data/temp.local /data/rc.local
chmod 755 /data/rc.local
