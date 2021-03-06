#!/bin/bash

#cd $APP_DIR
. env/bin/activate
. .envvars

# Init the database
#export TESTING=1
echo "TESTING"

# Evaluate the mode of execution and the 
if [[ $MODE == "SERVICE" ]]
    then
    # Run celery worker
    ./env/bin/celery worker -A app.celery_tasks -c 1 -n $APP_NAME"_celery_test_app"  & > tests_celery.log
    # Run gunicorn tests
    echo "Starting $APP_NAME in SERVICE testing.."
    ./env/bin/python -m app.tests.tests_service
    # Kill celery worker
    kill $(ps ax | grep $APP_NAME"_celery_test_app" | fgrep -v grep | awk '{ print $1 }')
elif [[ $MODE == "CONSUMER" ]]
    then
    # Run as consumer
    echo "Starting $APP_NAME in CONSUMER testing..."
    ./env/bin/python -m app.tests.tests_consumer
elif [[ $MODE == "TASK" ]]
    then
    echo "Starting $APP_NAME in TASK testing..."
    ./env/bin/python -m app.tests.test_task
fi

