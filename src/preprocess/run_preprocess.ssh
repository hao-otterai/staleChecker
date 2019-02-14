#!/bin/bash

# cluster
#$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-10:7077 \
#--conf spark.yarn.executor.memoryOverhead=600 \
#--executor-memory 6G \
#--driver-memory 6G preprocess.py

#local
$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-10.us-west-2.compute.internal:7077 \
--executor-memory 6G src/preprocess/preprocess.py
