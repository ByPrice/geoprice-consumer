#!/bin/bash

# if app dir not set, set to app name
if [ -z "$APP_DIR" ]
    then
    APP_DIR="/"$APP_NAME
fi

echo "Root app dir: $APP_DIR"
cd $APP_DIR
/bin/bash $APP_DIR/env/bin/activate

# Init the database
$APP_DIR/env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run celery
    echo "Starting Cekery..."
    $APP_DIR/env/bin/celery worker -A app.celery_tasks -c 3 -n $APP_NAME"_"$RANDOM  & 
    # Run gunicorn
    echo "Starting $APP_NAME in SERVICE mode"
    $APP_DIR/env/bin/gunicorn --workers 3 --bind unix:geoprice.sock -m 000 wsgi:app &
    nginx -g "daemon off;"

elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    $APP_DIR/env/bin/flask consumer &

elif [[ $MODE == "TASK" ]]
    then
    # Run as task
    echo "Starting $APP_NAME in TASK mode"
    # Get argument of script name...
    $APP_DIR/env/bin/flask script --name=$SCRIPT
    
fi

