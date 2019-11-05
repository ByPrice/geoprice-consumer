#!/bin/bash

deactivate

source ../.envvars-data
source ./.envvars

pipenv run flask initdb

if [[ $MODE == "SERVICE" ]]
    then
    # Run celery
    echo "Starting Celery..."
    pipenv run celery worker -A app.celery_tasks -c 3 -n $APP_NAME"_"$RANDOM  & 
    # Run gunicorn
    echo "Starting Web Service"
    pipenv run gunicorn --workers 3 --bind unix:geoprice.sock -t 200 -m 000 wsgi:app &
    nginx -g "daemon off;"

elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting Consumer"
    pipenv run flask consumer

elif [[ $MODE == "TASK" ]]
    then
    # Run as task
    echo "Starting Script $SCRIPT"
    # Get argument of script name...
    pipenv run flask script --name=$SCRIPT
fi
