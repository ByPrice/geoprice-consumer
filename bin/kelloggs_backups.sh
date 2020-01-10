#!/bin/bash

echo "Running Kelloggs Backuos in S3 for past prices"

# ENV
source .envvars
source env/bin/activate

# Run
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-25
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-26
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-27
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-28
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-29
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-30
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-31
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-01
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-02
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-03
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-04
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-05
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-06
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-07
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-08
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-09
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-10
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-11
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-12
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-13
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-14
flask backups_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-15
