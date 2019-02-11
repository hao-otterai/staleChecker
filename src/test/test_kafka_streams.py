
import os
import sys
import redis
import json
import datetime

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config


# Converts incoming news, Adds timestamp to incoming question
### NB - a lot of preprocessing needs to be added here
def extract_data(data):
    data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return data



def main():

    spark_conf = SparkConf().setAppName("Spark Streaming Test")
    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    #ssc.checkpoint("_spark_streaming_checkpoint")

    kafka_stream = KafkaUtils.createDirectStream( ssc, [config.KAFKA_TOPIC],
            {"metadata.broker.list": ",".join(config.KAFKA_SERVERS)} )

    # Process stream
    parsed = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))

    # print the entire json
    print("===================================")
    parsed.pprint()

    df = parsed.foreachRDD(lambda rdd: rdd.foreachPartition(lambda r: r.toDF().pprint()))

    # count this batch
    count_this_batch = parsed.count().map(lambda x:('News this batch: %s' % x)).pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
