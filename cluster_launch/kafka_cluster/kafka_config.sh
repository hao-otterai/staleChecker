#!/bin/bash

####
"""
for zookeeper and kafka cluster configuration, I recommend the following blog:
https://medium.com/dhoomil-sheta/processing-streaming-twitter-data-using-kafka-and-spark-part-1-setting-up-kafka-cluster-6e491809fa6d

in order to use the following command, you'll need to properly install/config both zookeeper and kafka,
and use zkServer to start zookeeper on each node of the cluster.

"""

# create kafka topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 6 --topic dowjones

# check topic configuration
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic dowjones
