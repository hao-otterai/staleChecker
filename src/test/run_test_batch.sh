#!/bin/bash

# #remote
# spark-submit --executor-memory 6G --master spark://ec2-35-155-171-170.us-west-2.compute.amazonaws.com:7077 \
# src/batch_processing/batchCustomMinHashLSH.py

# local
spark-submit --master spark://ip-10-0-0-10.us-west-2.compute.internal:7077 \
--executor-memory 6G src/test/test_Batch_single_tag.py
