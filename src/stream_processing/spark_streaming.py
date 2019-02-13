
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
import batch_customMinHashLSH as batch_process


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def conver_rdd_to_df(rdd, input_schema):
    print("==== Converting RDD[json] to DataFrame ====")
    spark = getSparkSessionInstance(rdd.context.getConf())
    return  spark.createDataFrame(rdd, input_schema)


def process_mini_batch(rdd, input_schema):
    if rdd.isEmpty():
        print('rdd is empty')
        return

    # Create and save MinHash and LSH or load them from file
    mh, lsh = batch_process.load_mh_lsh()

    # convert dstream RDD to DataFrame. This is necessary for the preprocessing
    # which involves transform operations using MLlib
    df = conver_rdd_to_df(rdd, input_schema)
    if config.LOG_DEBUG:
        df.printSchema()
        print(df.first())

    # preprocess
    df_preprocessed = preprocess.df_preprocess_func(df)
    # calculate CustomMinHashLSH
    df_with_hash_sig = batch_process.compute_minhash_lsh(df_preprocessed, mh, lsh)
    # iterate over the news in each partition
    def _process_news_iter(iter):
        for news in iter: process_news(news)
    df_with_hash_sig.foreachPartition(_process_news_iter)



def process_news(news):
    """
    for each news, do the following for similarity comparison:
        1) filter news within the pass 3 days
        2) filter news with at least one common company tags (intersection)
        3) filter news with at least one common band
        4) filter news with jaccard sim score above threshold
        5) save result to redis and return
    """
    if len(news)==0: return
    if config.LOG_DEBUG:
        print("==========print news for testing============")
        print(news)

    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    tags = news["tag_company"]
    tq = []
    for tag in tags:
        #tq += rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
        tq += rdb.zrangebyscore("lsh:{0}".format(tag), int(tags['timestamp'])-config.TIME_WINDOW,
                                int(tags['timestamp']), withscores=False)
    df = sql_context.read.json(sc.parallelize(tq))

    udf_num_common_buckets = udf(lambda x: util.intersection(x, news['lsh_hash']), IntegerType())
    udf_get_jaccard_similarity = udf(lambda x: util.jaccard_sim_score(x, news['min_hash']), FloatType())
    df.withColumn('common_buckets', udf_num_common_buckets('lsh_hash')).filter(
            col('common_buckets') > config.LSH_SIMILARITY_BAND_COUNT).withColumn(
            'jaccard_sim', udf_get_jaccard_similarity('min_hash')).filter(
            col('jaccard_sim') > config.DUP_QUESTION_MIN_HASH_THRESHOLD)

    if config.LOG_DEBUG:
        cnt = df.count()
        print(cnt, df.first())

    # Store similar candidates in Redis
    def store_similar_cands_redis(similar_dict):
        """
        input - similar_dict:
            key: (id, headline, timestamp)
            val: [jaccard_sim,  (id, headline, timestamp)]
        """
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        for cand in similar_dict:
            token = "dup_cand:{}".format(cand[0])
            for sim in similar_dict[cand]:
                val = (cand[1], cand[2], sim[1])
                # Store by jaccard_sim_score
                rdb.zadd(token, sim[0], val)

    def save_dup_to_redis(iter, news):
        rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
        token = "dup_cand:{}".format(tuple(news['id'], news['headline'], news['timestamp']))
        for entry in iter:
            dup = tuple(entry.id, entry.headline, entry.timestamp)
            if config.LOG_DEBUG:
                print("Found candidate: {0}".format(dup))
            rdb.zadd(token, entry.jaccard_sim, dup)

    df.foreachPartition(lambda iter: save_dup_to_redis(iter, news))


#     # find set of news ids which have at least one common lsh bucket
#     #if config.LOG_DEBUG: df.printSchema()
#     rdd_common_bucket = df.select(col('id'), col('min_hash'), col('headline'), col('timestamp'), col('lsh_hash')).rdd.flatMap(
#         lambda x: (((hash, band), [(x[0], x[1], x[2], x[3])]) for band, hash in enumerate(x[4]))).reduceByKey(
#         lambda a, b: util.custom_extend(a,b)).filter(lambda x: len(x[1])>1).map(lambda x: tuple(x[1]))
#     #if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_common_bucket.collect()))
#
#     def _merge_result(acc_list, value_list):
#         # Remove redundant similar sets from each partitions
#         output = []
#         for _v in value_list+acc_list:
#             if _v not in output: output.append(_v)
#             #if config.LOG_DEBUG > 1: print('LSH.get_merge_result=> _final_dict=%s'%(_final_dict))
#         return output
#
#     rdd_cands = rdd_common_bucket.map(lambda cand_set: get_jaccard_similarity(cand_set))
#     #if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_cands.first()))
#
#     similar_sets_dict = rdd_cands.flatMap(lambda x: x.items()).reduceByKey(lambda acc, val: _merge_result(acc, val)).collectAsMap()
#     #if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(similar_sets_dict))
#
#     return similar_sets_dict


    # # Store tag + news in Redis
    # q_json = json.dumps({"id": q_id, "title": news["title"],
    #         "min_hash": tuple(q_mh), "lsh_hash": tuple(q_lsh),
    #         "timestamp": news["timestamp"]})
    # for tag in tags:
    #     # Fetch all questions from that tag
    #     tq_table_size = rdb.zcard("lsh:{0}".format(tag))
    #     rdb.zadd("lsh:{0}".format(tag), news['timestamp'], q_json)
    #     rdb.sadd("lsh_keys", "lsh:{0}".format(tag))
    #     # To find max tag (we're only evaluating this)
    #     if(tq_table_size > max_tag_table_size):
    #         max_tag = tag
    #         max_tag_table_size = tq_table_size
    # tag = max_tag

    # Abandon spark parallelization
    # can we improve on this?
    # print("Tag:{0}, Size: {1}".format(max_tag, max_tag_table_size))
    # if(max_tag_table_size >= config.DUP_QUESTION_MIN_TAG_SIZE):
    #     print("Comparing in Tag:{0}".format(tag))
    #     # Too slow
    #     tq_table = rdb.zrevrange("lsh:{0}".format(tag), 0, config.MAX_QUESTION_COMPARISON, withscores=False)
    #     tq = [json.loads(tq_entry) for tq_entry in tq_table]
    #
    #     for entry in tq:
    #         if(entry["id"] != q_id):
    #             lsh_comparison = lsh.common_bands_count(entry["lsh_hash"], q_lsh)
    #             if(lsh_comparison > config.LSH_SIMILARITY_BAND_COUNT):
    #                 mh_comparison = mh.jaccard_sim_score(entry["min_hash"], q_mh)
    #                 print("MH comparison:{0}".format(mh_comparison))
    #                 if(mh_comparison > config.DUP_QUESTION_MIN_HASH_THRESHOLD):
    #                     cand_reformatted = (tag, q_id, news["title"], entry["id"], entry["title"], news["timestamp"])
    #                     print("Found candidate: {0}".format(cand_reformatted))
    #                     rdb.zadd("dup_cand", mh_comparison, cand_reformatted)

# def newsRDD_to_DF(newsRDD):
#     # Convert RDD[String] to RDD[Row] to DataFrame
#     rowRdd = rdd.map(lambda w: Row(word=w))
#     wordsDataFrame = spark.createDataFrame(rowRdd)
#     return df


# def process_mini_batch(rdd, mh, lsh):
#     for news in rdd:
#         if len(news) > 0:
#             process_news(news, mh, lsh)


def main():

    spark_conf = SparkConf().setAppName("Spark Streaming MinHashLSH")
    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess/preprocess.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_processing/batch_customMinHashLSH.py")

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    #ssc.checkpoint("_spark_streaming_checkpoint")


    # Kafka stream
    kafka_stream = KafkaUtils.createDirectStream( ssc,
                    [config.KAFKA_TOPIC],
                    {"metadata.broker.list": ",".join(config.KAFKA_SERVERS)} )

    def _extract_data(data):
        data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
        return data

    # stream - parse the json
    dstream = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1])).map(lambda x: _extract_data(x))

    # count the number of news in the stream mini-batch
    if config.LOG_DEBUG:
        count_mini_batch = dstream.count().map(lambda x:('==== {} news in mini-batch ===='.format(x))).pprint()

    # preprocess the news
    # schema for converting input news stream RDD[json] to DataFrame
    input_schema = StructType([StructField(field, StringType(), nullable = True)
                        for field in config.INPUT_SCHEMA_FIELDS])
    dstream.foreachRDD(lambda rdd: process_mini_batch(rdd, input_schema))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
