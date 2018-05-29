#!/bin/bash

cd /$APP_NAME
/bin/bash ./env/bin/activate

# Init the database
./env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $APP_MODE == "SERVICE" ]]
    then
    # Run gunicorm
    echo "Starting $APP_NAME in SERVICE mode"
    env/bin/gunicorn --bind $APP_HOST:$APP_PORT -t 200 wsgi:app
    #nginx -g "daemon off;"
elif [[ $APP_MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER mode"
    ./env/bin/flask consumer
elif [[ $APP_MODE == "TASK" ]]
    then
    # Run as task
    echo "Starting $APP_NAME in TASK mode"
    # Call to execute script : $TASK_NAME
fi

