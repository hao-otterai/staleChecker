import sys
import os
import redis
import time

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.feature import MinHashLSH, VectorAssembler, HashingTF

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'

# Store LSH similarity data
def store_spark_mllib_sim_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for sim in rdd:
        if(sim.jaccard_sim > config.DUP_QUESTION_IDENTIFY_THRESHOLD):
            q_pair = (sim.tag, sim.q1_id, sim.q1_title, sim.q2_id, sim.q2_title)
            rdb.zadd("spark_mllib_sim", sim.jaccard_sim, q_pair)


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
    model.transform(vdf).show()

    # Approximate similarity join between pairwise elements
    find_tag = udf(lambda x, y: util.common_tag(x, y), StringType())

    if(config.LOG_DEBUG): print("[MLLIB BATCH]: Computing approximate similarity join...")
    sim_join = model.approxSimilarityJoin(vdf, vdf, config.DUP_QUESTION_MIN_HASH_THRESHOLD, distCol="jaccard_sim").select(
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
    sim_join.foreachPartition(store_spark_mllib_sim_redis)


def main():
    spark_conf = SparkConf().setAppName("Spark MLLib MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print("Spark MLLib MinHashLSH run time (seconds): {0}".format(end_time - start_time))


if(__name__ == "__main__"):
    main()
