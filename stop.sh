#!/bin/bash
source .envvars
# Stop Processes
kill -9 $(ps aux | grep flask | grep $APP_NAME | grep consum | awk '{print $2}')
# Clear Log Files
rm $PWD/logs/consumer_*.log
