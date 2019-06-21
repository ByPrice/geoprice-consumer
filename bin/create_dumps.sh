#!/bin/bash

# Script to run dumps in server

# Env
source /home/byprice/geoprice/.envvars
source /home/byprice/geoprice/env/bin/activate

# Run script
echo "Running Create Dumps .."
/home/byprice/geoprice/env/bin/flask script create_dumps