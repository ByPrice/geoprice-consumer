#!/bin/bash

# Script to run backups in server
/home/byprice/geoprice/
# Env
source /home/byprice/geoprice/.envvars
source /home/byprice/geoprice/env/bin/activate

# Run script
echo "Running Create Backups .."
/home/byprice/geoprice/env/bin/flask script --name=create_backups