#!/bin/bash

kill -9 $(ps ax | grep geoprice/env/bin/gunicorn | fgrep -v grep | awk '{ print $1 }')
kill -9 $(ps ax | grep geoprice/env/bin/celery | fgrep -v grep | awk '{ print $1 }')

source env/bin/activate
source .envvars

# Run celery
nohup celery worker -A app.celery_app -c 4 -n $APP_NAME"_"$RANDOM > logs/celery.log 2>&1 & 
echo "Celery Worker running..."

nohup gunicorn --workers 6 --bind unix:$APP_NAME.sock -m 000 -t 240 wsgi:app  > logs/gunicorn.log 2>&1 &
