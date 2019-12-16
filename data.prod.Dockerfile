FROM byprice/base-data-services:v2_3.6.8

# Copy service content
COPY ./ /geoprice/
RUN mkdir /logs

# Change workdir
WORKDIR /geoprice

# Install project dependencies
RUN pipenv install

VOLUME /var/log/geoprice

# App , environment & Logging
ENV APP_NAME='geoprice-data-service-production'
ENV APP_DIR='/'
# Bug with rabbit_engine file , not adding _dev for LOCAL
ENV ENV='PROD'
ENV FLASK_APP=app/__init__.py
ENV REGION='MEX'
ENV LOG_LEVEL='ERROR'

# Streamer
ENV STREAMER='rabbitmq'
ENV STREAMER_HOST='rmq-prod.byprice.com'
ENV STREAMER_PORT=5222
ENV STREAMER_QUEUE='geoprice'
ENV STREAMER_ROUTING_KEY='geoprice'
ENV STREAMER_EXCHANGE='data'
ENV STREAMER_EXCHANGE_TYPE='direct'
ENV STREAMER_VIRTUAL_HOST='mx'
ENV STREAMER_USER='mx_pubsub'
# ENV STREAMER_PASS from secret

ENV SCRIPT='create_stats'
ENV TASK_ARG_CREATE_DUMPS="kelloggs,ims,paris"

# Queues
ENV QUEUE_CACHE='cache'
ENV QUEUE_ROUTING='routing'
ENV QUEUE_CATALOGUE='catalogue'
ENV QUEUE_CATALOGUE_ITEM='catalogue_item'
ENV QUEUE_GEOPRICE='geoprice'
ENV QUEUE_GEOLOCATION='geolocation'

# Celert
ENV C_FORCE_ROOT='true'

# Celery
ENV CELERY_REDIS_DB='5'

# Cassandra
ENV CASSANDRA_CONTACT_POINTS='34.83.36.166'
ENV CASSANDRA_KEYSPACE='geoprice'
ENV CASSANDRA_PASSWORD='byprice'
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_USER='byprice'
#ENV CASSANDRA_PASSWORD from secret

#ENTRYPOINT /bin/bash /geoprice/bin/run_consumer.sh
