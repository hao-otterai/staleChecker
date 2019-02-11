
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


# schema for converting input news stream RDD[json] to DataFrame
global input_schema
input_schema = StructType([StructField(field, StringType(), nullable = True)
                    for field in config.INPUT_SCHEMA_FIELDS])

def extract_data(data):
    data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return data


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def rdd2df(rdd, input_schema):
    print("==== rdd2df: Converting RDD[json] to DataFrame ====")
    spark = getSparkSessionInstance(rdd.context.getConf())
    return  spark.createDataFrame(rdd, input_schema)


def preprocess(df):
    # Clean question body
    clean_body = udf(lambda body: preprocess.filter_body(body), StringType())
    df_cleaned = df.withColumn("cleaned_body", preprocess.clean_body("body"))

    # generate tags based on company, industry, and market
    tag_generator = udf(lambda input_string: preprocess.generate_tag(input_string), ArrayType(StringType()))
    df_tagged = df_cleaned.withColumn( "tag_company",  tag_generator("company"))

    # Concat cleaned question body and question title to form question vector
    df_textbody = df_tagged.withColumn("text_body", concat(col("headline"), lit(" "), col("cleaned_body")))

    # Tokenize question title
    tokenizer = Tokenizer(inputCol="text_body", outputCol="text_body_tokenized")
    df_tokenized = tokenizer.transform(df_textbody)

    # Remove stop words
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    df_stopword = stop_words_remover.transform(df_tokenized)

    # Stem words
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    df_stemmed = df_stopword.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))

    # Timestamp
    final_data = df_stemmed.withColumn("display_timestamp",unix_timestamp("display_date", "yyyyMMdd'T'HHmmss.SSS'Z'"))

    return final_data



def compute_minhash_lsh(df):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    if config.LOG_DEBUG: print(df.first())
    #df.foreachPartition(store_lsh_redis_by_topic)
    return df



def get_similar_cands_tags():
    """
    """
    pass


def main_process(rdd, input_schema):
    if rdd.isEmpty():
        print('rdd is empty')
    else:
        df = rdd2df(rdd, input_schema)
        #df.printSchema()
        #print(df.first())
        df_pre = preprocess(df)
        df_hash = compute_minhash_lsh(df_pre)

        get_similar_cands_tags(df_hash)
        df_hash.map()


# def newsRDD_to_DF(newsRDD):
#     # Convert RDD[String] to RDD[Row] to DataFrame
#     rowRdd = rdd.map(lambda w: Row(word=w))
#     wordsDataFrame = spark.createDataFrame(rowRdd)
#     return df


# def process_news(news):
#     if len(news)==0: return
#     rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
#
#     df = newsRDD_to_DF(news)
#
#     df_prep = news_preprocess(df)
#     if config.LOG_DEBUG: print(df_prep.collect())
#
#     df_hash = compute_minhash_lsh(df_prep)
#     if config.LOG_DEBUG: print(df_hash.collect())
#
#     q_id = news["id"]
#     q_mh = mh.calc_min_hash_signature(news["text_body_stemmed"])
#     q_lsh = lsh.find_lsh_buckets(q_mh)
#
#     tags = news["tags"].split("|")
#     max_tag = ""
#     max_tag_table_size = 0
#
#     for tag in tags:
#         # Fetch all questions from that tag
#         tq_table_size = rdb.zcard("lsh:{0}".format(tag))
#
#         # Store tag + news in Redis
#         q_json = json.dumps({"id": q_id, "title": news["title"], "min_hash": tuple(q_mh), "lsh_hash": tuple(q_lsh), "timestamp": news["timestamp"]})
#         rdb.zadd("lsh:{0}".format(tag), news["view_count"], q_json)
#         rdb.sadd("lsh_keys", "lsh:{0}".format(tag))
#
#         # To find max tag (we're only evaluating this)
#         if(tq_table_size > max_tag_table_size):
#             max_tag = tag
#             max_tag_table_size = tq_table_size
#
#     tag = max_tag
#
#     # Abandon spark parallelization
#     # can we improve on this?
#     print("Tag:{0}, Size: {1}".format(max_tag, max_tag_table_size))
#     if(max_tag_table_size >= config.DUP_QUESTION_MIN_TAG_SIZE):
#         print("Comparing in Tag:{0}".format(tag))
#         # Too slow
#         tq_table = rdb.zrevrange("lsh:{0}".format(tag), 0, config.MAX_QUESTION_COMPARISON, withscores=False)
#         tq = [json.loads(tq_entry) for tq_entry in tq_table]
#
#         for entry in tq:
#             if(entry["id"] != q_id):
#                 lsh_comparison = lsh.common_bands_count(entry["lsh_hash"], q_lsh)
#                 if(lsh_comparison > config.LSH_SIMILARITY_BAND_COUNT):
#                     mh_comparison = mh.jaccard_sim_score(entry["min_hash"], q_mh)
#                     print("MH comparison:{0}".format(mh_comparison))
#                     if(mh_comparison > config.DUP_QUESTION_MIN_HASH_THRESHOLD):
#                         cand_reformatted = (tag, q_id, news["title"], entry["id"], entry["title"], news["timestamp"])
#                         print("Found candidate: {0}".format(cand_reformatted))
#                         rdb.zadd("dup_cand", mh_comparison, cand_reformatted)


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
    ssc.checkpoint("_spark_streaming_checkpoint")

    # Create and save MinHash and LSH or load them from file
    ### NB should mh and lsh be broadcasted???
    global mh
    global lsh
    mh, lsh = load_mh_lsh()

    # Kafka stream
    kafka_stream = KafkaUtils.createDirectStream( ssc,
                    [config.KAFKA_TOPIC],
                    {"metadata.broker.list": ",".join(config.KAFKA_SERVERS)} )

    # stream - parse the json
    parsed_stream = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))

    if config.LOG_DEBUG: # count the number of news in the stream mini-batch
        count_this_batch = parsed_stream.count().map(
            lambda x:('==== {} news in mini-batch ===='.format(x))).pprint()

    # preprocess the news
    parsed_stream.foreachRDD(lambda rdd: main_process(rdd, input_schema))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
