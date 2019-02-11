
import os
import sys
import redis
import json
import datetime

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config


# Converts incoming news, Adds timestamp to incoming question
### NB - a lot of preprocessing needs to be added here
def extract_data(data):
    data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return data


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(
            conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def rdd2df(rdd):
    print("===================================")
    print("rdd2df: Converting RDD[json] to DataFrame...")
    spark = getSparkSessionInstance(rdd.context.getConf())
    df = spark.createDataFrame(rdd)
    df.printSchema()
    print(df.first())

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

    print("===================================")
    # count this batch
    count_this_batch = parsed.count().map(lambda x:('News this batch: %s' % x)).pprint()
    parsed.pprint()

    spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()
    df = parsed.foreachRDD(rdd2df)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
