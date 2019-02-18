
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
from pyspark.sql.types import *

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config


global input_schema
#fields = fieldList.map(lambda fieldName: StructField(fieldName, StringType(), nullable = true))
input_schema = StructType([StructField(_fieldName, StringType(), nullable = True) for _fieldName in config.INPUT_SCHEMA_FIELDS])


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


def process(rdd):
    if rdd.isEmpty():
        print('rdd is empty')
    else:
        df = rdd2df(rdd)
        df.printSchema()
        print(df.first())

def rdd2df(rdd):
    print("=========== rdd2df: Converting RDD[json] to DataFrame =========")
    spark = getSparkSessionInstance(rdd.context.getConf())
    return  spark.createDataFrame(rdd, input_schema)


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

    # count this batch
    count_this_batch = parsed.count().map(lambda x:('=========== Num of news in the batch: %s  ==========' % x)).pprint()
    parsed.pprint()

    parsed.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
