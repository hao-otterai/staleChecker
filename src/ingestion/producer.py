import os
import sys
import kafka
import time
import threading
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

# from pyspark import SparkContext
# from pyspark.conf import SparkConf
# from pyspark.sql import SQLContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils


class Producer(threading.Thread):
    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=config.KAFKA_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version=(0, 10))

        file_dir = "/home/ubuntu/staleChecker/src/ingestion/2001_sample_10M_stream_pre.json"
        #json_file = sql_context.read.json(file_dir).collect()
        with open(file_dir) as f: json_file = json.load(f)

        for line in json_file:
            if config.LOG_DEBUG: print(line)
            producer.send(config.KAFKA_TOPIC, line)
            time.sleep(config.KAFKA_CONSUMER_REFRESH)

def main():
    producer = Producer()
    producer.start()
    print("Starting Kafka Producer...")

    # spark_conf = SparkConf().setAppName("Producer")
    # global sc
    # sc = SparkContext(conf=spark_conf)
    # global sql_context
    # sql_context = SQLContext(sc)

if __name__ == "__main__":
    main()
