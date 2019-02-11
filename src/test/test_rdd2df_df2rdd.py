import os
import sys
import redis
import json
import datetime

from pyspark import SparkContext
from pyspark.conf import SparkConf

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/stream_processing")

import config
import spark_streaming

def main():

    spark_conf = SparkConf().setAppName("Spark Streaming Test")
    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/stream_processing/spark_streaming.py")

    file_dir = "/home/hao/staleChecker/src/ingestion/2001_sample_10M_stream.json"
    with open(file_dir) as f: json_file = json.load(f)
    rdd = sc.parallelize(json_file)

    df = spark_streaming.rdd2df(rdd, spark_streaming.input_schema)
    df.printSchema()
    print(df.first())

    # play with dataframe
    def f(iterator):
        for ii in iterator:
            print(ii)
    df.rdd.map(list).foreachPartition(f)


    back2rdd = df.rdd.map(list)
    print(back2rdd.first())

if __name__ == "__main__":
    main()
