#!/bin/bash

echo "Running Kelloggs Statistics for past prices"

# ENV
source .envvars
source env/bin/activate

# Run
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-25
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-26
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-27
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-28
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-29
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-30
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-05-31
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-01
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-02
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-03
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-04
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-05
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-06
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-07
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-08
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-09
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-10
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-11
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-12
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-13
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-14
flask stats_retailer --rets=kelloggs_walmart,kelloggs_chedraui --date=2019-06-15
