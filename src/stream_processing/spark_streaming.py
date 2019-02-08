
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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/preprocess")

import config
import util
import locality_sensitive_hash
import min_hash
import preprocess

# Converts incoming news, Adds timestamp to incoming question
### NB - a lot of preprocessing needs to be added here
def extract_data(data):
    data["ingest_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return data

def news_preprocess(news):
    df = news.toDF()
    # Clean question body
    clean_body = udf(lambda body: preprocess.filter_body(body), StringType())
    partially_cleaned_data = df.withColumn("cleaned_body", preprocess.clean_body("body"))

    # generate tags based on company, industry, and market
    tag_generator = udf(lambda input_string: preprocess.generate_tag(input_string), ArrayType(StringType()))
    partially_cleaned_data = partially_cleaned_data.withColumn( "tag_company",  tag_generator("company"))

    # Concat cleaned question body and question title to form question vector
    data = partially_cleaned_data.withColumn("text_body", concat(col("headline"), lit(" "), col("cleaned_body")))

    # Tokenize question title
    tokenizer = Tokenizer(inputCol="text_body", outputCol="text_body_tokenized")
    tokenized_data = tokenizer.transform(data)

    # Remove stop words
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    stop_words_removed_data = stop_words_remover.transform(tokenized_data)

    # Stem words
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    stemmed_data = stop_words_removed_data.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))
    final_data = stemmed_data
    return final_data

def compute_minhash_lsh(df, mh, lsh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    #if config.LOG_DEBUG: print(df.first())
    df.foreachPartition(store_lsh_redis_by_topic)
    return df

def process_news(news, mh, lsh):
    if len(news)==0: return
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    news_preprocessed = news_preprocess(news)
    if config.LOG_DEBUG: print(news_preprocessed.collect())

    news_hashed = compute_minhash_lsh(news_preprocessed, mh, lsh)
    if config.LOG_DEBUG: print(news_hashed.collect())

    # q_id = news["id"]
    # q_mh = mh.calc_min_hash_signature(news["text_body_stemmed"])
    # q_lsh = lsh.find_lsh_buckets(q_mh)
    #
    # tags = news["tags"].split("|")
    # max_tag = ""
    # max_tag_table_size = 0
    #
    # for tag in tags:
    #     # Fetch all questions from that tag
    #     tq_table_size = rdb.zcard("lsh:{0}".format(tag))
    #
    #     # Store tag + news in Redis
    #     q_json = json.dumps({"id": q_id, "title": news["title"], "min_hash": tuple(q_mh), "lsh_hash": tuple(q_lsh), "timestamp": news["timestamp"]})
    #     rdb.zadd("lsh:{0}".format(tag), news["view_count"], q_json)
    #     rdb.sadd("lsh_keys", "lsh:{0}".format(tag))
    #
    #     # To find max tag (we're only evaluating this)
    #     if(tq_table_size > max_tag_table_size):
    #         max_tag = tag
    #         max_tag_table_size = tq_table_size
    #
    # tag = max_tag
    #
    # # Abandon spark parallelization
    # # can we improve on this?
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


# Compute MinHash
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

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    ssc.checkpoint("_spark_streaming_checkpoint")

    kafka_stream = KafkaUtils.createDirectStream( ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_SERVERS} )

    # Create and save MinHash and LSH or load them from file
    if (not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = min_hash.MinHash(config.MIN_HASH_K_VALUE)
        lsh = locality_sensitive_hash.LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)

        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    # Process stream
    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(lambda entry: process_news(entry, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
