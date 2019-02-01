#!/bin/bash

# cluster
$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 6G --driver-memory 6G spark_mllib_tag_indexed_batch.py

# local
# $SPARK_HOME/bin/spark-submit --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 spark_mllib_tag_indexed_batch.py