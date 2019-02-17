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
    if news is None or len(news) == 0:
        return
    if config.LOG_DEBUG:
        print('========= process_news: {} ======='.format(news['headline']))

    q_timestamp = int(news['timestamp'])
    q_mh = mh.calc_min_hash_signature(news['text_body_stemmed'])
    q_lsh = lsh.find_lsh_buckets(q_mh)

    """
    get the dataframe for all news of given tag. id and lsh_hash columns loaded from Redis.
    """
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    dup_cands = {}
    ids = set()
    for tag in news['tag_company']:
        ids = ids.union(rdb.smembers("lsh:{}".format(tag)))

    if len(ids) > 0:
        if config.LOG_DEBUG:
            print('Comparing with {} previous news of same tag(s)'.format(len(ids)))

        for id in ids:
            # time windowing
            temp_timestamp = rdb.hget("news:{}".format(id), 'timestamp')
            if temp_timestamp is None: continue
            temp_timestamp = int(temp_timestamp)
            if temp_timestamp > q_timestamp or temp_timestamp < (q_timestamp-config.TIME_WINDOW): continue

            # LSH bucketing
            temp_lsh  = rdb.hget("news:{}".format(id), 'lsh_hash')
            if temp_lsh is  None: continue
            temp_lsh = [int(i) for i in temp_lsh.split(',')]
            if util.sim_count(q_lsh, temp_lsh) < config.LSH_SIMILARITY_BAND_COUNT: continue

            # Jaccard similarity calculation
            temp_mh   = rdb.hget("news:{}".format(id), 'min_hash')
            if temp_mh is None: continue
            temp_mh = [int(i) for i in temp_mh.split(',')]
            jaccard_sim = util.jaccard_sim_score(q_hm, temp_mh)
            if jaccard_sim < config.DUP_QUESTION_MIN_HASH_THRESHOLD: continue

            dup_cands[id] = jaccard_sim

        # save dup_cand to Redis
        performance_metrics = {}
        performance_metrics['ingest_timestamp'] = news['ingest_timestamp']
        performance_metrics['consum_timestamp'] = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        performance_metrics['num_comps'] = len(ids)
        rdb.hmget('dup_cand_performance:{}'.format(news['id']), performance_metrics)
        if len(dup_cands)>0:
            rdb.hmset('dup_cand:{}'.format(news['id']), dup_cands)

    # save input news to Redis
    for tag in news['tag_company']:
        rdb.sadd("lsh:{}".format(tag), news['id'])
        rdb.sadd("lsh_keys", "lsh:{}".format(tag))
    rdb.hmset("news:{}".format(news['id']),
                {
                    "headline": news['headline'],
                    "min_hash": ",".join([str(i) for i in q_mh]),
                    "lsh_hash": ",".join([str(i) for i in q_lsh]),
                    "timestamp": q_timestamp,
                    "tag_company": ",".join(news["tag_company"])
                }
            )


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
                .foreachRDD( lambda rdd: rdd.foreachPartition(lambda data: process_news(data, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
