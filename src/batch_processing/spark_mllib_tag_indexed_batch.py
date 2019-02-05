import sys
import os
import time
import json

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.ml.feature import MinHashLSH, VectorAssembler, HashingTF, IDF

import redis

from pyspark.sql.types import IntegerType, ArrayType, StringType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

# calculate TF-IDF
def compute_tf_idf(documents):
    hashingTF = HashingTF()
    tf = hashingTF.transform(documents)
    # While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    # First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    idf = IDF(minDocFreq=config.MIN_DOC_FREQ).fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

# Store question data
def store_lsh_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for q in rdd:
        for tag in q.tag_company:
            q_json = json.dumps({"id": q.id, "headline": q.headline, "min_hash": q.min_hash, "lsh_hash": q.lsh_hash, "timestamp": q.display_timestamp })
            rdb.zadd("lsh:{0}".format(tag), q.display_timestamp, q_json)
            rdb.sadd("lsh_keys", "lsh:{0}".format(tag))


# Computes MinHashes, LSHes for all in DataFrame
def compute_minhash_lsh(df, mh, lsh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    df.foreachPartition(store_lsh_redis)


# Store duplicate candidates in Redis
def store_dup_cand_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for cand in rdd:
        cand_reformatted = (cand.q1_id, cand.q1_headline, cand.q2_id, cand.q2_headline, cand.q1_timestamp, cand.q2_timestamp)
        # Store by time
        rdb.zadd("dup_cand", cand.mh_js, cand_reformatted)


# Store LSH similarity data
def store_spark_mllib_tag_indexed_sim_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for sim in rdd:
        if(sim.jaccard_sim > config.DUP_QUESTION_IDENTIFY_THRESHOLD):
            q_pair = (sim.tag, sim.q1_id, sim.q1_headline, sim.q2_id, sim.q2_headline, sim.q1_timestamp, sim.q2_timestamp, sim.timediff)
            rdb.zadd("spark_mllib_tag_indexed_sim", sim.jaccard_sim, q_pair)


# Compares LSH signatures, MinHash signature, and find duplicate candidates
def find_dup_cands_within_tags(model):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # Fetch all tags from lsh_keys set
    for lsh_key in rdb.sscan_iter("lsh_keys", match="*", count=500):
        tag = lsh_key.replace("lsh:", "")

        tq = rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
        if config.LOG_DEBUG: print("{0}: {1} news".format(tag, len(tq)))
        tq_df = sql_context.read.json(sc.parallelize(tq))

        if(config.LOG_DEBUG): print("[MLLIB BATCH]: Computing approximate similarity join...")
        find_tag = udf(lambda x, y: util.common_tag(x, y), StringType())
        cal_timediff = udf(lambda x, y: abs(x-y), IntegerType())
        sim_join = model.approxSimilarityJoin(tq_df, tq_df, config.DUP_QUESTION_MIN_HASH_THRESHOLD, distCol="jaccard_sim").select(
            col("datasetA.id").alias("q1_id"),
            col("datasetB.id").alias("q2_id"),
            col("datasetA.headline").alias("q1_headline"),
            col("datasetB.headline").alias("q2_headline"),
            col("datasetA.text_body_vectorized").alias("q1_text_body"),
            col("datasetB.text_body_vectorized").alias("q2_text_body"),
            col("datasetA.timestamp").alias("q1_timestamp"),
            col("datasetB.timestamp").alias("q2_timestamp"),
            cal_timediff("datasetA.timestamp", "datasetB.timestamp").alias("timediff"),
            find_tag("datasetA.tag_company", "datasetB.tag_company").alias("tag"),
            col("jaccard_sim")
        )
        sim_join = sim_join.filter(sim_join.timediff < config.TIME_WINDOW)

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

    if(config.LOG_DEBUG): print("[MLLIB BATCH]: Fitting MinHashLSH model...")
    model = mh.fit(vdf)

    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print("[BATCH]: Fetching questions in same tag, \
        comparing LSH and MinHash, uploading duplicate candidates back to Redis...")
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
    print("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time))


if(__name__ == "__main__"):
    main()
