#!/bin/bash
rm -rf preprocess/__pycache__
rm -rf stream_processing/_spark_streaming_checkpoint
rm -rf lib/*.pickle
rm -rf lib/__pycache__
rm -rf batch_processing/spark_warehouse
rm -rf config/__pycache__
rm -rf stream_processing/__pycache__
rm -rf batch_processing/__pycache__
rm **/*.pyc
