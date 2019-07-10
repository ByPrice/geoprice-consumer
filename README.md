# Byprice GeoPrice Service

Web App for Geographic Price Consultation in Cassandra using  direct and async queries, and Consumer App to write into Cassandra retrieved infom,  and a  Web Service requests into async requests with Celery using Redis as a broker. 

## Deployments

The **Geoprice Services** run in `GCP` with the following specifications:

- **PROD**
  - **Web Service**
    - *GCP Compute Engine Instance*: `geoprice-byprice-prod`
    - *Public Endpoint*: `geoprice.byprice.com/geoprice/`
    - *Allowed Protocols*: `HTTP`
    - *Repository Path*: `/home/byprice/geoprice`
    - *Web Application Init Script File*: `/home/byprice/geoprice/restart.sh`
    - *Web Application Log File*: `/home/byprice/geoprice/logs/gunicorn.log`
    - *Celery Workers Log File*: `/home/byprice/geoprice/logs/celery.log`
    - *Cron Jobs Logs Directory*: `/home/byprice/geoprice/logs`
    - *DB Technology*:  `Cassandra 3.11.4 `
    - *DB Hosts*:  `cassandra-cluster-db-vm-0`, `cassandra-cluster-db-vm-1`, `cassandra-cluster-db-vm-2`, `cassandra-cluster-db-vm-3` (Running all in **GCP** )
    - *DB User:* `byprice`
    - *DB Name*: `geoprice`
    - *Broker Technology*: `Redis 3.0.6`
    - *Broker Host*: `localhost`  (Running in the same instance as the Athena Service)
    - *Broker DB:* `5`
    - *NginX Server File*: `/etc/nginx/sites-available/services`
    - *Python Version*: `3.5.2`
  - **Consumer**
    - *GCP Compute Engine Instance*: `geogeoprice-byprice-consumer-prod-2`
    - *Repository Path*: `/home/byprice/geoprice`
    - *Consumer Init Script File*: `/home/byprice/geoprice/run_consumer.sh`
    - *Consumer Logs Directory*: `/home/byprice/geoprice/logs/`
    - *DB Technology*:  `Cassandra 3.11.4 `
    - *DB Hosts*:  `cassandra-cluster-db-vm-0`, `cassandra-cluster-db-vm-1`, `cassandra-cluster-db-vm-2`, `cassandra-cluster-db-vm-3` (Running all in **GCP** )
    - *DB User:* `byprice`
    - *DB Name*: `geoprice`
    - *Consumer Broker Technology*: `RabbitMQ 3.2.4`
    - *Consumer Broker Hosts*: `rabbitmq-cluster-prod-1`, `rabbitmq-cluster-prod-2`, `rabbitmq-cluster-prod-3`  (Running all in **GCP**)
    - *Consumer Broker Queue:* `bp_geoprice`
    - *Python Version*: `3.6.7`
- **DEV**
    - **Web Service**
        - *GCP Compute Engine Instance*: `geoprice-athena-dev`
        - *Public Endpoint*: `dev.geoprice.byprice.com/geoprice/`
        - *Allowed Protocols*: `HTTP`
        - *Repository Path*: `/home/byprice/geoprice`
        - *Web Application Init Script File*: `/home/byprice/geoprice/restart.sh`
        - *Web Application Log File*: `/home/byprice/geoprice/logs/gunicorn.log`
        - *Celery Workers Log File*: `/home/byprice/geoprice/logs/celery.log`
        - *Cron Jobs Logs Directory*: `/home/byprice/geoprice/logs`
        - *DB Technology*:  `Cassandra 3.11.4 `
        - *DB Hosts*:  `cassandra-cluster-db-vm-0`, `cassandra-cluster-db-vm-1`, `cassandra-cluster-db-vm-2`, `cassandra-cluster-db-vm-3` (Running all in **GCP** )
        - *DB User:* `byprice`
        - *DB Name*: `geoprice`
        - *Broker Technology*: `Redis 3.0.6`
        - *Broker Host*: `localhost`  (Running in the same instance as the Athena Service)
        - *Broker DB:* `5`
        - *NginX Server File*: `/etc/nginx/sites-available/services`
        - *Python Version*: `3.6.8`
    - **Consumer**
        - TODO


### Reverse Proxy Configurations (NginX)

- Configurations are located in the `nginx/` directory:
  - PROD: `nginx/prod/services`
  - DEV: `nginx/dev/services`

They have to be configured in its respective GCP servers to be located in the NginX folder for deployment.

### Cron Jobs

#### Web Service

In the `geoprice-byprice-prod` instance of **GCP** , where the Web Service is running, there are three main cron jobs that  need to be executed :

- Create Daily Stats Cron: `crons/geoprice_crons.txt` (line 3) 
- Create Backups Cron: `crons/geoprice_crons.txt`  (line 4)
- Create Intel Dumps Cron: `crons/geoprice_crons.txt`  (line 5)

#### Consumer

In the `geogeoprice-byprice-consumer-prod-2` instance of **GCP**, where the Consumers are running, to avoid issues on connection latency with remote RabbitMQ, the following cron is run to restart consumers.

- Restart Consumers Cron: `crons/geoprice_consumers_cron.txt`

Crons have to be set using `crontab -e` in the respective server, and modify the paths and  permissions  of the respective shell scripts.

-----

## Development Setup 

### Pre-requirements

- Python>=3.6
- Redis>=3.0.6
- Cassandra==3.11

### Installation

- Create Virtualenv and install Python reqs.

```bash
# Virtualenv setup 
virtualenv env
. env/bin/activate
pip install -r requirements.txt
```

- Set up environmental variables: To run in a local computer you can use a shell script containing the following and name the file `.envvars`:

```bash
#!/bin/bash
#!/bin/bash


# APP
export APP_MODE="SERVICE"
export MODE=$APP_MODE
export APP_PORT=8000
export APP_NAME="geoprice"
export FLASK_APP="app/__init__.py"
export SCRIPT="create_stats"
export TASK_ARG_CREATE_DUMPS="paris"

# Broker
export CELERY_BROKER="redis"
export CELERY_HOST="localhost"
export CELERY_PORT=6379
export CELERY_PASSWORD=""
export CELERY_REDIS_DB=5

# Result Backend
export TASK_BACKEND="redis"
export REDIS_HOST="localhost"
export REDIS_PORT=6379
export REDIS_PASSWORD=""
export REDIS_DB=5

# Cassandra
export CASSANDRA_CONTACT_POINTS="0.0.0.0"
export CASSANDRA_KEYSPACE="geoprice"
export CASSANDRA_PORT=9042
export CASSANDRA_USER="byprice"
export CASSANDRA_PASSWORD=""

# Env
export ENV="DEV"
export LANG="C.UTF-8"
export LC_ALL="C.UTF-8"

# Logs
export LOG_HOST="localhost"
export LOG_LEVEL="DEBUG"

# Streamer 
export STREAMER="rabbitmq"
export STREAMER_HOST="localhost"
export STREAMER_PORT=5672    
export STREAMER_VIRTUAL_HOST=""
export STREAMER_USER="guest"
export STREAMER_PASS="guest"
export QUEUE_ROUTING="bp_routing"
export QUEUE_GEOPRICE="bp_geoprice"
export QUEUE_CACHE="bp_cache"

# Services
export SRV_CATALOGUE="gate.byprice.com/bpcatalogue"
export SRV_GEOLOCATION="gate.byprice.com/bpgeolocation"

# AWS Credentials
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
```

- Finally run these in a shell script to execute server:

```bash
# Run Redis Server in background
redis-server &
source .envvars
source env/bin/activate
# Start Celery
celery worker -A app.celery_app --loglevel="INFO" -c 1 -n "$APP_NAME""_""$RANDOM"
# Only For Local
gunicorn --workers 1 --bind 0.0.0.0:8000 -t 200 -m 000 wsgi:app
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

## Schema 

[See details](./schema.cql)


## Contracts

[See details](./CONTRACTS.md)

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
