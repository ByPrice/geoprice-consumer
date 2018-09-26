#!/bin/bash

#cd $APP_DIR
. env/bin/activate
. .envvars

# Init the database
#export TESTING=1

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run gunicorn tests
    echo "Starting $APP_NAME in SERVICE testing.."
    ./env/bin/python -m app.tests.tests_service
elif [[ $MODE == "CONSUMER" ]]
    then
    # Run celery worker
    ./env/bin/celery worker -A app.celery_tasks -c 1 -n $APP_NAME"_celery_test_app"  &
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER testing..."
    ./env/bin/python -m app.tests.tests_consumer
    echo "Starting $APP_NAME in TASK testing..."
    ./env/bin/python -m app.tests.test_task
    # Kill celery worker
    kill $(ps ax | grep $APP_NAME"_celery_test_app" | fgrep -v grep | awk '{ print $1 }')
fi

