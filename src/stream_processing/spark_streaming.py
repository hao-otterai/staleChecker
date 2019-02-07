
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
import config
import util
from MinHash import MinHash
from LSH import LSH

# Converts incoming question, Adds timestamp to incoming question
### NB - a lot of preprocessing needs to be added here
def extract_data(data):
    data["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return data


def process_question(question, mh, lsh):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    q_id = question["id"]
    q_mh = mh.calc_min_hash_signature(question["text_body_stemmed"])
    q_lsh = lsh.find_lsh_buckets(q_mh)

    tags = question["tags"].split("|")
    max_tag = ""
    max_tag_table_size = 0
    for tag in tags:
        # Fetch all questions from that tag
        tq_table_size = rdb.zcard("lsh:{0}".format(tag))

        # Store tag + question in Redis
        q_json = json.dumps({"id": q_id, "title": question["title"], "min_hash": tuple(q_mh), "lsh_hash": tuple(q_lsh), "timestamp": question["timestamp"]})
        rdb.zadd("lsh:{0}".format(tag), question["view_count"], q_json)
        rdb.sadd("lsh_keys", "lsh:{0}".format(tag))

        # To find max tag (we're only evaluating this)
        if(tq_table_size > max_tag_table_size):
            max_tag = tag
            max_tag_table_size = tq_table_size

    tag = max_tag

    # Abandon spark parallelization
    # can we improve on this?
    print("Tag:{0}, Size: {1}".format(max_tag, max_tag_table_size))
    if(max_tag_table_size >= config.DUP_QUESTION_MIN_TAG_SIZE):
        print("Comparing in Tag:{0}".format(tag))
        # Too slow
        tq_table = rdb.zrevrange("lsh:{0}".format(tag), 0, config.MAX_QUESTION_COMPARISON, withscores=False)
        tq = [json.loads(tq_entry) for tq_entry in tq_table]

        for entry in tq:
            if(entry["id"] != q_id):
                lsh_comparison = lsh.common_bands_count(entry["lsh_hash"], q_lsh)
                if(lsh_comparison > config.LSH_SIMILARITY_BAND_COUNT):
                    mh_comparison = mh.jaccard_sim_score(entry["min_hash"], q_mh)
                    print("MH comparison:{0}".format(mh_comparison))
                    if(mh_comparison > config.DUP_QUESTION_MIN_HASH_THRESHOLD):
                        cand_reformatted = (tag, q_id, question["title"], entry["id"], entry["title"], question["timestamp"])
                        print("Found candidate: {0}".format(cand_reformatted))
                        rdb.zadd("dup_cand", mh_comparison, cand_reformatted)


# Compute MinHash
def process_mini_batch(rdd, mh, lsh):
    for question in rdd:
        if len(question) > 0:
            process_question(question, mh, lsh)


def main():

    spark_conf = SparkConf().setAppName("Spark Streaming MinHash")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/MinHash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/LSH.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    ssc.checkpoint("_spark_streaming_checkpoint")

    kafka_stream = KafkaUtils.createDirectStream(
        ssc,
        [config.KAFKA_TOPIC],
        {"metadata.broker.list": config.KAFKA_SERVERS}
    )

    # Create and save MinHash and LSH or load them from file
    if (not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = MinHash(config.MIN_HASH_K_VALUE)
        lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)

        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    # Process stream
    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(lambda question: process_mini_batch(question, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
