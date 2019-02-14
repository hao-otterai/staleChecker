import os
import sys
import redis
import json
import datetime

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_processing")

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import config
import util
import locality_sensitive_hash
import min_hash
import preprocess
import batchCustomMinHashLSH as batch_process


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def conver_rdd_to_df(rdd, input_schema):
    print("==== Converting RDD[json] to DataFrame ====")
    spark = getSparkSessionInstance(rdd.context.getConf())
    return  spark.createDataFrame(rdd, input_schema)


def process_mini_batch(rdd, input_schema, mh, lsh):
    if not rdd.isEmpty():
        # convert dstream RDD to DataFrame. This is necessary for the preprocessing
        # which involves transform operations using MLlib
        df = conver_rdd_to_df(rdd, input_schema)
        if config.LOG_DEBUG:
            df.printSchema()
            print(df.first())

        try:
            # preprocess
            df_preprocessed = preprocess.df_preprocess_func(df)
            # calculate CustomMinHashLSH
            df_with_hash_sig = batch_process.compute_minhash_lsh(df_preprocessed, mh, lsh)
            # iterate over the news in each partition
            df_with_hash_sig.foreachPartition(process_news)
        except Exception as e:
            print(e)


def process_news(iter):
    """
    for each news, do the following for similarity comparison:
        1) filter news within the pass 3 days
        2) filter news with at least one common company tags (intersection)
        3) filter news with at least one common band
        4) filter news with jaccard sim score above threshold
        5) save result to redis and return
    """
    def helper(iter, news):
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        token = "dup_cand:{}".format(news.id)
        for entry in iter:
            processed_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            dup = tuple(news.headline, news.timestamp, entry.id, entry.headline,
                            entry.timestamp, entry.ingest_timestamp, processed_timestamp)
            rdb.zadd(token, entry.jaccard_sim, dup)

    for news in iter:
        if len(news)==0: return
        if config.LOG_DEBUG:
            print("==========print news for testing============")
            print(news)

        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        # tags = news["tag_company"]
        tq = []
        for tag in news.tag_company:
            #tq += rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
            tq += rdb.zrangebyscore("lsh:{0}".format(tag), long(news.timestamp)-config.TIME_WINDOW,
                                    long(news.timestamp), withscores=False)
        tq = list(set(tq))
        df = sql_context.read.json(sc.parallelize(tq))

        udf_num_common_buckets = udf(lambda x: util.intersection(x, news.lsh_hash), IntegerType())
        udf_get_jaccard_similarity = udf(lambda x: util.jaccard_sim_score(x, news.min_hash), FloatType())
        df.withColumn('common_buckets', udf_num_common_buckets('lsh_hash')).filter(
                col('common_buckets') > config.LSH_SIMILARITY_BAND_COUNT).withColumn(
                'jaccard_sim', udf_get_jaccard_similarity('min_hash')).filter(
                col('jaccard_sim') > config.DUP_QUESTION_MIN_HASH_THRESHOLD)

        if config.LOG_DEBUG:
            df.count().map(lambda x: "{} similar news found".format(x))

        # # Store similar candidates in Redis
        df.foreachPartition(lambda iter: helper(iter, news))


def main():

    spark_conf = SparkConf().setAppName("Spark Streaming MinHashLSH")
    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess/preprocess.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_processing/batchCustomMinHashLSH.py")

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    #ssc.checkpoint("_spark_streaming_checkpoint")

    # Kafka stream
    kafka_stream = KafkaUtils.createDirectStream( ssc,
                    [config.KAFKA_TOPIC],
                    {"metadata.broker.list": ",".join(config.KAFKA_SERVERS)} )

    def _ingest_timestamp(data):
        data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        return data

    # stream - parse the json
    dstream = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1])).map(lambda x: _ingest_timestamp(x))

    # count the number of news in the stream mini-batch
    if config.LOG_DEBUG:
        count_mini_batch = dstream.count().map(lambda x:('==== {} news in mini-batch ===='.format(x))).pprint()

    # Create and save MinHash and LSH or load them from file
    mh, lsh = batch_process.load_mh_lsh()

    # preprocess the news
    # schema for converting input news stream RDD[json] to DataFrame
    input_schema = StructType([StructField(field, StringType(), nullable = True)
                        for field in config.INPUT_SCHEMA_FIELDS])
    dstream.foreachRDD(lambda rdd: process_mini_batch(rdd, input_schema, mh, lsh))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
