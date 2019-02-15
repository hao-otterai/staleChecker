import os
import sys
import redis
import json
import datetime
import time

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_processing")


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
    start_time = time.time()
    if config.LOG_DEBUG: print("==== Converting RDD[json] to DataFrame ====")
    spark = getSparkSessionInstance(rdd.context.getConf())
    end_time = time.time()
    if config.LOG_DEBUG: print("conver_rdd_to_df run time (seconds): {0} seconds".format(end_time - start_time))
    return  spark.createDataFrame(rdd, input_schema)


# Store news data
def store_lsh_redis_by_tag(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    if config.LOG_DEBUG: print("store minhash and lsh by company tag")
    for q in rdd:
        q_json = json.dumps({"id": q.id, "headline": q.headline, "min_hash": q.min_hash,
                    "lsh_hash": q.lsh_hash, "timestamp": q.timestamp })
        try:
            for tag in q.tag_company:
                rdb.zadd("lsh:{0}".format(tag), q.timestamp, q_json)
                rdb.sadd("lsh_keys", "lsh:{0}".format(tag))
        except Exception as e:
            print("ERROR: failed to save tag {0} to Redis".format(tag))



def compute_minhash_lsh(df):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    #if config.LOG_DEBUG: print(df.first())
    #df.foreachPartition(store_lsh_redis_by_tag)
    return df


def save2redis(iter, news):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    token = "dup_cand:{}".format(news['id']) # id
    for entry in iter:
        processed_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        dup = tuple(news['headline'], news['timestamp'], entry.id, entry.headline,
                        entry.timestamp, news['ingest_timestamp'], processed_timestamp)
        rdb.zadd(token, entry.jaccard_sim, dup)



def test_func(iter,  mh, lsh):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for news in iter:
        if len(news) > 0:
            q_timestamp = long(news['timestamp'])
            q_mh = mh.calc_min_hash_signature(news['text_body_stemmed']) #
            q_lsh = lsh.find_lsh_buckets(q_mh)
            tags = news['tag_company']

            # Store tag + news in Redis
            q_json = json.dumps({"id": news['id'], "headline": news['headline'], "min_hash": tuple(q_mh),
                                "lsh_hash": tuple(q_lsh), "timestamp": q_timestamp})
            for tag in tags:
                rdb.zadd("lsh:{0}".format(tag), q_timestamp, q_json)
                rdb.sadd("lsh_keys", "lsh:{0}".format(tag))

            tq = []
            for tag in tags:
                tq += rdb.zrangebyscore("lsh:{0}".format(tag),q_timestamp-config.TIME_WINDOW, q_timestamp, withscores=False)
            tq = list(set(tq))
            df = sql_context.read.json(sc.parallelize(tq))

            udf_num_common_buckets = udf(lambda x: util.sim_count(x, q_lsh), IntegerType())
            udf_get_jaccard_similarity = udf(lambda x: util.jaccard_sim_score(x, q_mh), FloatType())
            filtered_df = df.withColumn('common_buckets', udf_num_common_buckets('lsh_hash')).filter(
                    col('common_buckets') > config.LSH_SIMILARITY_BAND_COUNT).withColumn(
                    'jaccard_sim', udf_get_jaccard_similarity('min_hash')).filter(
                    col('jaccard_sim') > config.DUP_QUESTION_MIN_HASH_THRESHOLD)

            if config.LOG_DEBUG:
                filtered_df.count().map(lambda x: "{} similar news found".format(x))

            filtered_df.foreachPartition(lambda iter: save2redis(iter, news))

    # tq_table = rdb.zrevrange("lsh:{0}".format(tag), 0, config.MAX_QUESTION_COMPARISON, withscores=False)
    # tq = [json.loads(tq_entry) for tq_entry in tq_table]
    #
    # for entry in tq:
    #     if(entry["id"] != q_id):
    #         lsh_comparison = lsh.common_bands_count(entry["lsh_hash"], q_lsh)
    #         if(lsh_comparison > config.LSH_SIMILARITY_BAND_COUNT):
    #             mh_comparison = mh.jaccard_sim_score(entry["min_hash"], q_mh)
    #             print(colored("MH comparison:{0}".format(mh_comparison), "blue"))
    #             if(mh_comparison > config.DUP_QUESTION_MIN_HASH_THRESHOLD):
    #                 cand_reformatted = (tag, q_id, question["title"], entry["id"], entry["title"], question["timestamp"])
    #                 print(colored("Found candidate: {0}".format(cand_reformatted), "cyan"))
    #                 rdb.zadd("dup_cand", mh_comparison, cand_reformatted)




def process_mini_batch(rdd, input_schema, mh, lsh):
    if not rdd.isEmpty():
        # convert dstream RDD to DataFrame. This is necessary for the preprocessing
        # which involves transform operations using MLlib
        df = conver_rdd_to_df(rdd, input_schema)
        if config.LOG_DEBUG: print(df.first())

        # preprocess
        df_preprocess = preprocess.df_preprocess_func(df)

        # calculate CustomMinHashLSH
        df_with_hash_sig = compute_minhash_lsh(df_preprocess, mh, lsh)

        # # Extract data that we want
        # df_preprocess.registerTempTable("df_preprocess")
        # _output_fields = "id, headline, body, text_body, text_body_stemmed, tag_company, source, hot, display_date, timestamp, djn_urgency"
        # global sql_context
        # df_preprocess_final = sql_context.sql( "SELECT {} from df_preprocess".format(_output_fields) )

        # try:
        df_with_hash_sig.foreachPartition(process_news)
        # except Exception as e:
        #     print("process_mini_batch error: ", e)



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
        try:
            if len(news)>0:
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
        except Exception as e:
            print(e)


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

    # schema for converting input news stream RDD[json] to DataFrame
    #input_schema = StructType([StructField(field, StringType(), nullable = True) for field in config.INPUT_SCHEMA_FIELDS])

    def _ingest_timestamp(data):
        data['ingest_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        return data


    def test_process_mini_batch(rdd, mh, lsh):
        rdd.foreachPartition(lambda iter: test_func(iter, mh, lsh))

    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1])).map(
        lambda x: _ingest_timestamp(x)).foreachRDD(
        lambda rdd: test_process_mini_batch(rdd, mh, lsh))


    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
