#!/bin/bash

# Environment
source .envvars
source env/bin/activate

# Init the database
./env/bin/flask initdb

# Execute migration
python -m app.scripts.migration $@


