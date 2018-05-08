FROM byprice/base-python:latest

# Copy repo
COPY ./ /byprice-geoprice/
RUN mkdir /byprice-geoprice/logs

# Change workdir
WORKDIR /byprice-geoprice

# Install local dependencies
RUN npm install

# Map ports
EXPOSE 8000
EXPOSE 80

# Add Nginx configuration file
ADD cfn/nginx/conf.d/ /etc/nginx/conf.d
RUN rm -rf /etc/nginx/sites-available/default && rm -rf /etc/nginx/sites-enabled/default

ENTRYPOINT /bin/bash /byprice-geoprice/bin/run.sh