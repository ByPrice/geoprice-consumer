#!/bin/bash
export APP_MODE="CONSUMER"
export MODE=$APP_MODE
echo "[$(date)][GEOPRICE]: Activating virtual environment"

# todo ?? Init the database
pipenv run flask initdb


is_running_gunicorn=$(ps aux | grep 'gunicorn' | wc -l)
if [ $is_running_gunicorn -gt 1 ]
    then
            echo "[$(date)][$APP_NAME]: Already running with GUNICORN in $MODE mode . Shutting down"
            ps aux | grep 'gunicorn'
            kill $(ps aux | grep 'gunicorn' | awk '{print $2}')
fi


is_running_celery=$(ps aux | grep 'celery' | wc -l)
if [ $is_running_celery -gt 1 ]
    then
            echo "[$(date)][$APP_NAME]: Already running with CELERY in $MODE mode . Shutting down"
            ps aux | grep 'celery'
            kill $(ps aux | grep 'celery' | awk '{print $2}')
fi

is_running_flask_run=$(ps aux | grep 'flask consumer' | wc -l)
if [ $is_running_flask_run -gt 1 ]
    then
            echo "[$(date)][$APP_NAME]: Already running $APP_NAME with Flask Consumer in $MODE mode . Shutting down"
            ps aux | grep 'flask consumer'
            kill $(ps aux | grep 'flask consumer' | awk '{print $2}')
fi


echo "[$(date)][$APP_NAME]: Starting with FLASK in $MODE mode..."
pipenv run flask consumer