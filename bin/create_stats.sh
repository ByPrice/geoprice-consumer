#!/bin/bash

# Script to run stats in server

# Env
source /home/byprice/geoprice/.envvars
source /home/byprice/geoprice/env/bin/activate

# Run script
echo "Running Create Stats .."
/home/byprice/geoprice/env/bin/flask script create_stats