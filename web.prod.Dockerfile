FROM byprice/base-web-services:v2_3.6

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

ENV APP_PORT=8080

# Celert
ENV C_FORCE_ROOT='true'

# Services
ENV SRV_CATALOGUE='gate.byprice.com/bpcatalogue'
ENV SRV_GEOLOCATION='gate.byprice.com/bpgeolocation'
ENV SRV_PROTOCOL='http'

# Cassandra
ENV CASSANDRA_CONTACT_POINTS='35.233.244.27'
ENV CASSANDRA_KEYSPACE='geoprice'
ENV CASSANDRA_PASSWORD='byprice'
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_USER='byprice'
#ENV CASSANDRA_PASSWORD from secret

# Celery
ENV CELERY_BROKER='redis'
ENV CELERY_HOST='localhost'
ENV CELERY_PORT='6379'
ENV CELERY_PASSWORD=''
ENV CELERY_REDIS_DB='2'

# Celery - Backend
ENV TASK_BACKEND='redis'
ENV REDIS_HOST='localhost'
ENV REDIS_PORT='6379'
ENV REDIS_PASSWORD=''
ENV REDIS_DB='2'
#ENV REDIS_PASSWORD from secret