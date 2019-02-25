#!/bin/bash

spark-submit --master spark://ip-10-0-0-10.us-west-2.compute.internal:7077  --executor-memory 6G \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0  \
src/stream_processing/spark_streaming.py
