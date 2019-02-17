import os
import sys
import redis
import json
from datetime import datetime
import time

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, concat, col, lit, unix_timestamp
from pyspark.ml.feature import StopWordsRemover, Tokenizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_processing")
import config
import util
import locality_sensitive_hash
import min_hash
import preprocess
import batchCustomMinHashLSH as batch_process


# schema for converting input news stream RDD[json] to DataFrame
#input_schema = StructType([StructField(field, StringType(), nullable = True) for field in config.INPUT_SCHEMA_FIELDS])


def save2redis(iter, news):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    token = "dup_cand:{}".format(news['id'])
    for entry in iter:
        processed_timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        dup = (news['headline'], news['timestamp'], entry.id, entry.headline, entry.timestamp,
                    news['ingest_timestamp'], processed_timestamp)
        if config.LOG_DEBUG: print("saving dup_cand to Redis: {}".format(dup))
        rdb.zadd(token, entry.jaccard_sim, dup)



def _ingest_timestamp(data):
    output = data
    output['ingest_timestamp'] = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
    return output



def process_news(news, mh, lsh):
    if config.LOG_DEBUG: print('========= process_news: {} ======='.format(news['headline']))

    q_timestamp = int(news['timestamp'])
    q_mh = mh.calc_min_hash_signature(news['text_body_stemmed'])
    q_lsh = lsh.find_lsh_buckets(q_mh)

    """
    get the dataframe for all news of given tag. id and lsh_hash columns loaded from Redis.
    """
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    tq = []
    for tag in news['tag_company']:
        #tq += rdb.zrangebyscore("lsh:{0}".format(tag),q_timestamp-config.TIME_WINDOW, q_timestamp, withscores=False)
        for id in rdb.smembers("lsh:{}".format(tag)):
            temp_lsh       = rdb.hget("news:{}".format(id), 'lsh_hash')
            temp_mh        = rdb.hget("news:{}".format(id), 'min_hash')
            temp_timestamp = rdb.hget("news:{}".format(id), 'timestamp')
            if temp_lsh is not None and temp_mh is not None:
                temp = {}
                temp['id'] = id
                temp['lsh_hash'] = [int(i) for i in temp_lsh.split(',')]
                temp['min_hash'] = [int(i) for i in temp_mh.split(',')]
                temp['timestamp'] = int(temp_timestamp)
                tq.append(temp)
            else:
                print("Failed to get lsh_hash for news:{}".format(id))
    if len(tq) < 1: return
    df = sql_context.read.json(sc.parallelize(tq))

    if config.LOG_DEBUG:
        print("{0} historical news in the tag(s): {1}".format(len(tq), news['tag_company']))
        df.pprint()

    udf_num_common_buckets = udf(lambda x: util.sim_count(x, q_lsh), IntegerType())
    udf_get_jaccard_similarity = udf(lambda x: util.jaccard_sim_score(x, q_mh), FloatType())
    filtered_df = df.filter((col('timestamp') > (q_timestamp-config.TIME_WINDOW)) & (col('timestamp') < q_timestamp))\
            .withColumn('common_buckets', udf_num_common_buckets('lsh_hash'))\
            .filter( col('common_buckets') > config.LSH_SIMILARITY_BAND_COUNT)\
            .withColumn( 'jaccard_sim', udf_get_jaccard_similarity('min_hash'))\
            .filter( col('jaccard_sim') > config.DUP_QUESTION_MIN_HASH_THRESHOLD)
    #if config.LOG_DEBUG: filtered_df.count().map(lambda x: "{} similar news found".format(x))
    filtered_df.foreachPartition(lambda iter: save2redis(iter, news))


    """ Store tag + news in Redis """
    if config.LOG_DEBUG: print('========== Save news data to Redis =============')
    for tag in news['tag_company']:
        # rdb.zadd("lsh:{}".format(tag), q.timestamp, json.dumps(news_data))
        rdb.sadd("lsh:{}".format(tag), news['id'])
        rdb.sadd("lsh_keys", "lsh:{}".format(tag))
    news_data = {
                    "headline": news['headline'],
                    "min_hash": ",".join([str(i) for i in q_mh]),
                    "lsh_hash": ",".join([str(i) for i in q_lsh]),
                    "timestamp": q_timestamp,
                    "tag_company": ",".join(news["tag_company"])
                }
    rdb.hmset("news:{}".format(news['id']), news_data)



# # Lazily instantiated global instance of SparkSession
# def getSparkSessionInstance(sparkConf):
#     if ("sparkSessionSingletonInstance" not in globals()):
#         globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
#     return globals()["sparkSessionSingletonInstance"]
#
#


# def conver_rdd_to_df(rdd, input_schema):
#     start_time = time.time()
#     if config.LOG_DEBUG: print("==== Converting RDD[json] to DataFrame ====")
#     spark = getSparkSessionInstance(rdd.context.getConf())
#     end_time = time.time()
#     if config.LOG_DEBUG: print("conver_rdd_to_df run time (seconds): {0} seconds".format(end_time - start_time))
#     return  spark.createDataFrame(rdd, input_schema)


# def process_news(iter):
#     """
#     for each news, do the following for similarity comparison:
#         1) filter news within the pass 3 days
#         2) filter news with at least one common company tags (intersection)
#         3) filter news with at least one common band
#         4) filter news with jaccard sim score above threshold
#         5) save result to redis and return
#     """
#     def helper(iter, news):
#         rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
#         token = "dup_cand:{}".format(news.id)
#         for entry in iter:
#             processed_timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
#             dup = tuple(news.headline, news.timestamp, entry.id, entry.headline,
#                             entry.timestamp, entry.ingest_timestamp, processed_timestamp)
#             rdb.zadd(token, entry.jaccard_sim, dup)
#
#     for news in iter:
#         try:
#             if len(news)>0:
#                 rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
#                 # tags = news["tag_company"]
#                 tq = []
#                 for tag in news.tag_company:
#                     #tq += rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
#                     tq += rdb.zrangebyscore("lsh:{0}".format(tag), long(news.timestamp)-config.TIME_WINDOW,
#                                             long(news.timestamp), withscores=False)
#                 tq = list(set(tq))
#                 df = sql_context.read.json(sc.parallelize(tq))
#
#                 udf_num_common_buckets = udf(lambda x: util.intersection(x, news.lsh_hash), IntegerType())
#                 udf_get_jaccard_similarity = udf(lambda x: util.jaccard_sim_score(x, news.min_hash), FloatType())
#                 df.withColumn('common_buckets', udf_num_common_buckets('lsh_hash')).filter(
#                         col('common_buckets') > config.LSH_SIMILARITY_BAND_COUNT).withColumn(
#                         'jaccard_sim', udf_get_jaccard_similarity('min_hash')).filter(
#                         col('jaccard_sim') > config.DUP_QUESTION_MIN_HASH_THRESHOLD)
#
#                 if config.LOG_DEBUG:
#                     df.count().map(lambda x: "{} similar news found".format(x))
#
#                 # # Store similar candidates in Redis
#                 df.foreachPartition(lambda iter: helper(iter, news))
#         except Exception as e:
#             print(e)



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

    global sql_context
    sql_context = SQLContext(sc)

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    #ssc.checkpoint("_spark_streaming_checkpoint")

    mh, lsh = batch_process.load_mh_lsh()

    kafka_stream = KafkaUtils.createDirectStream( ssc,
                    [config.KAFKA_TOPIC],
                    {"metadata.broker.list": ",".join(config.KAFKA_SERVERS)} )

    kafka_stream.count().map(lambda x:('==== {} news in mini-batch ===='.format(x))).pprint()

    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
                .map(lambda data: _ingest_timestamp(data))\
                .foreachRDD( lambda rdd: rdd.foreach(lambda data: process_news(data, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
