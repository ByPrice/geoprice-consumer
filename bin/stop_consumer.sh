#!/bin/bash

# Stop Consumer Processes
kill -9 $(ps aux | grep flask | grep consum | awk '{print $2}')