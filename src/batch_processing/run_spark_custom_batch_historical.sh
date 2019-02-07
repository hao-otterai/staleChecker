#!/bin/bash

spark-submit --executor-memory 6G --master spark://ec2-35-155-171-170.us-west-2.compute.amazonaws.com:7077 \
src/batch_processing/spark_custom_batch_historical_news.py
