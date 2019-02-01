import sys
import os
import time
import json
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.ml.feature import MinHashLSH, VectorAssembler, HashingTF

import redis

from pyspark.sql.types import IntegerType, ArrayType, StringType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util


# Store question data
def store_lsh_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for q in rdd:
        tags = q.tags.split("|")
        for tag in tags:
            q_json = json.dumps({"id": q.id, "title": q.title, "min_hash": q.min_hash, "lsh_hash": q.lsh_hash})
            rdb.zadd("lsh:{0}".format(tag), q.view_count, q_json)
            rdb.sadd("lsh_keys", "lsh:{0}".format(tag))


# Computes MinHashes, LSHes for all in DataFrame
def compute_minhash_lsh(df, mh, lsh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    df.foreachPartition(store_lsh_redis)


# Store duplicate candidates in Redis
def store_dup_cand_redis(tag, rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for cand in rdd:
        cand_reformatted = (tag, cand.q1_id, cand.q1_title, cand.q2_id, cand.q2_title)
        # Store by time
        rdb.zadd("dup_cand", cand.mh_js, cand_reformatted)


# Store LSH similarity data
def store_spark_mllib_tag_indexed_sim_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for sim in rdd:
        if(sim.jaccard_sim > config.DUP_QUESTION_IDENTIFY_THRESHOLD):
            q_pair = (sim.tag, sim.q1_id, sim.q1_title, sim.q2_id, sim.q2_title)
            rdb.zadd("spark_mllib_tag_indexed_sim", sim.jaccard_sim, q_pair)


# Compares LSH signatures, MinHash signature, and find duplicate candidates
def find_dup_cands_within_tags(model):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # Fetch all tags from lsh_keys set
    for lsh_key in rdb.sscan_iter("lsh_keys", match="*", count=500):
        tag = lsh_key.replace("lsh:", "")
        tq_table_size = rdb.zcard("lsh:{0}".format(tag))
        if(tq_table_size >= config.DUP_QUESTION_MIN_TAG_SIZE):  # Ignore extremely small tags
            tq = rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
            if config.LOG_DEBUG: print(colored("{0}: {1} question(s)".format(tag, len(tq)), "yellow"))
            tq_df = sql_context.read.json(sc.parallelize(tq))

            if(config.LOG_DEBUG): print(colored("[MLLIB BATCH]: Computing approximate similarity join...", "green"))
            find_tag = udf(lambda x, y: util.common_tag(x, y), StringType())
            sim_join = model.approxSimilarityJoin(tq_df, tq_df, config.DUP_QUESTION_MIN_HASH_THRESHOLD, distCol="jaccard_sim").select(
                col("datasetA.id").alias("q1_id"),
                col("datasetB.id").alias("q2_id"),
                col("datasetA.title").alias("q1_title"),
                col("datasetB.title").alias("q2_title"),
                col("datasetA.text_body_vectorized").alias("q1_text_body"),
                col("datasetB.text_body_vectorized").alias("q2_text_body"),
                find_tag("datasetA.tags", "datasetB.tags").alias("tag"),
                col("jaccard_sim")
            )

            # Upload LSH similarities to Redis
            sim_join.foreachPartition(store_spark_mllib_tag_indexed_sim_redis)


def run_minhash_lsh():
    df = util.read_all_json_from_bucket(sql_context, config.S3_BUCKET_BATCH_PREPROCESSED)

    mh = MinHashLSH(inputCol="text_body_vectorized", outputCol="min_hash", numHashTables=config.LSH_NUM_BANDS)

    # Vectorize so we can fit to MinHashLSH model

    htf = HashingTF(inputCol="text_body_stemmed", outputCol="raw_features", numFeatures=1000)
    htf_df = htf.transform(df)

    vectorizer = VectorAssembler(inputCols=["raw_features"], outputCol="text_body_vectorized")
    vdf = vectorizer.transform(htf_df)

    if(config.LOG_DEBUG): print(colored("[MLLIB BATCH]: Fitting MinHashLSH model...", "green"))
    model = mh.fit(vdf)

    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching questions in same tag, comparing LSH and MinHash, uploading duplicate candidates back to Redis...", "cyan"))
    find_dup_cands_within_tags(model)


def main():
    spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print(colored("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
