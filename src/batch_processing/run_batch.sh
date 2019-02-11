#!/bin/bash

# #remote
# spark-submit --executor-memory 6G --master spark://ec2-35-155-171-170.us-west-2.compute.amazonaws.com:7077 \
# src/batch_processing/batch_customMinHashLSH.py

# local
spark-submit --executor-memory 6G src/batch_processing/batch_customMinHashLSH.py
