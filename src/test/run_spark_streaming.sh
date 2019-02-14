#!/bin/bash

# cluster
#spark-submit --master spark://ip-10-0-0-10:7077 \
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
#--jars /usr/lib/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/spark/lib/datanucleus-core-3.2.10.jar,/usr/lib/spark/lib/datanucleus-rdbms-3.2.9.jar \
#spark_streaming.py

# local
spark-submit --master spark://ip-10-0-0-10.us-west-2.compute.internal:7077 \
--executor-memory 6G \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
src/test/test_kafka_streams.py
