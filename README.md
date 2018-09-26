# Byprice GeoPrice Service

Web App for Geographic Price Consultation in Cassandra using  direct and async queries, and Consumer App to write into Cassandra retrieved info.

## Pre-requirements

- Python 3.4
- Redis 2.8.4

## Initialization

### Development

- Create Virtualenv and install Python reqs.

```bash
#!/usr/bin/env bash
sudo apt-get -y update

# Virtualenv installation
sudo apt-get -y install python3-pip
sudo pip3 install virtualenv

virtualenv env
. env/bin/activate
pip install -r requirements.txt
```

- Install Redis

```bash
sudo apt-get install redis-server
```

- Set up environmental variables: To run in a local computer you can use a shell script containing the following and name the file `.envvars`:

```bash
#!/bin/bash
export APP_MODE="SERVICE" # CONSUMER|TASKS
export APP_HOST="localhost"
export APP_PORT=8060
export DEBUG=True
export APP_DIR="$PWD"
export CELERY_BROKER="redis"
export CELERY_HOST="localhost"
export CELERY_PORT=6379
export ENV="LOCAL"
export CASSANDRA_CONTACT_POINTS="0.0.0.0"
export CASSANDRA_KEYSPACE="geoprice"
export CASSANDRA_PORT=9042
export LOG_HOST="localhost"
export LOG_LEVEL="DEBUG"
export STREAMER="rabbitmq"
export STREAMER_HOST="localhost"
export STREAMER_PORT=5672
export APP_NAME="geoprice"
export SRV_GEOLOCATION="gate.byprice.com/geo"
export SRV_CATALOGUE="gate.byprice.com/catalogue"
```

- Finally run these in a shell script to execute server:

```bash
# Run Redis Server in background
redis-server &
source .envvars
source env/bin/activate
# Start Celery
celery worker -A app.celery -n "$APP_NAME""_""$RANDOM" --loglevel=INFO --concurrency=1
# Only For Local
python wsgi.py
```

## Build

Build Docker image from file.

```bash
docker build -t geoprice --no-cache .
```

Tag the image with the AWS prefix, and environment tag (`dev` or `production`)

```bash
docker tag geoprice <aws_prefix>/geoprice:dev
```

Push Image to ECS

```bash
docker push <aws_prefix>/geoprice:dev
```


## Contracts

[See details](../CONTRACTS.md)

## Errors

Error codes with respective description.

- 80000 : "Bad Request"
- 80001 : "Connection Issues with DB"
- 80002 : "Request params missing"
- 80003 : "Request Key param missing"
- 80004 : "Not Found"
- 80005 : "Issues fetching results"
- 80006 : "Task Method not available"
- 80007 : "Invalid query parameter, try again!"
- 80008 : "File does not exist"
- 80009 : "No prices available in {table}"
- 80010 : "Wrong params format"
- 80011 : "No {results} found!"
- 80012 : "Incorrect data format, should be {format}"
- 89999 : "Internal error."

## License

Copyright (c) 2018 ByPrice.

## To Do

[Check here](./NOTES.md)
