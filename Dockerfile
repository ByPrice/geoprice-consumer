FROM byprice/base-python-web:latest

# Copy repo
COPY ./ /geoprice/
RUN mkdir /geoprice/logs

# Change workdir
WORKDIR /geoprice

# Install local dependencies
RUN virtualenv env && env/bin/pip install -r requirements.txt

# Map ports
EXPOSE 8000
EXPOSE 80

# Add Nginx configuration file
ADD cfn/nginx/conf.d/ /etc/nginx/conf.d
RUN rm -rf /etc/nginx/sites-available/default && rm -rf /etc/nginx/sites-enabled/default

ENTRYPOINT /bin/bash /geoprice/bin/run.sh