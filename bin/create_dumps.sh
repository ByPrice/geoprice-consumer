#!/bin/bash

# Script to run dumps in server
cd /home/byprice/geoprice/
# Env
source /home/byprice/geoprice/.envvars
source /home/byprice/geoprice/env/bin/activate

# Run script
echo "Running Create Dumps .."
/home/byprice/geoprice/env/bin/flask script --name=create_dumps