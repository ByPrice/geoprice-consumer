#!/bin/bash

cd $APP_DIR
/bin/bash ./env/bin/activate

# Init the database
./env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run celery
    echo "Starting Cekery..."
    ./env/bin/celery worker -A app.celery_tasks -c 3 -n $APP_NAME"_"$RANDOM  & 
    # Run gunicorn
    echo "Starting $APP_NAME in SERVICE mode"
    ./env/bin/gunicorn --workers 3 --bind unix:geoprice.sock -m 000 wsgi:app &
    nginx -g "daemon off;"

elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    ./env/bin/flask consumer &

elif [[ $MODE == "SCRIPT" ]]
    then
    # Run as task
    echo "Starting $APP_NAME in TASK mode"
    # Get argument of task name... $TASK_NAME
    ./env/bin/flask script --name=$TASK
    
fi

