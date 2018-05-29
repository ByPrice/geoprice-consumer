#!/bin/bash

#cd $APP_DIR
. env/bin/activate
. .envvars

# Init the database
./env/bin/flask initdb

# Evaluate the mode of execution and the 
if [[ $APP_MODE == "SERVICE" ]]
    then
    # Run gunicorn tests
    echo "Starting $APP_NAME in SERVICE testing.."
    ./env/bin/python -m app.tests.tests_service
elif [[ $APP_MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER testing..."
    ./env/bin/python -m app.tests.tests_consumer
elif [[ $APP_MODE == "TASK" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in TASK testing..."
    echo "TASKS TESTS HASN'T BEEN DEVELOPED YET"
fi

