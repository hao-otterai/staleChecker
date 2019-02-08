import sys
import os
import time
import json
import itertools
import redis

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, ArrayType, StringType, Row

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")

import config
import util

import locality_sensitive_hash
import min_hash


def load_mh_lsh():
    #  Create and save MinHash and LSH if not exist or load them from file
    if(not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = min_hash.MinHash(config.MIN_HASH_K_VALUE)
        lsh = locality_sensitive_hash.LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
        print('saving mh, lsh to file {}, {}'.format(config.MIN_HASH_PICKLE, config.LSH_PICKLE))
        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        if config.LOG_DEBUG: print('loading mh and lsh from local files')
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)
    if config.LOG_DEBUG: print('mh and lsh init finished')
    return mh, lsh


# Computes MinHashes, LSHes for all in DataFrame
def compute_minhash_lsh(df, mh, lsh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    #if config.LOG_DEBUG: print(df.first())
    df.foreachPartition(store_lsh_redis_by_topic)
    return df

# Store question data
def store_lsh_redis_by_topic(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    if config.LOG_DEBUG: print("store minhash and lsh by topic(i.e, company)")
    for q in rdd:
        q_json = json.dumps({"id": q.id, "headline": q.headline, "min_hash": q.min_hash,
                    "lsh_hash": q.lsh_hash, "timestamp": q.display_timestamp })
        #if config.LOG_DEBUG: print("Storing the tags {0} and content {1} to Redis...".format(q.tag_company, q_json))
        try:
            for tag in q.tag_company:
                rdb.zadd("lsh:{0}".format(tag), q.display_timestamp, q_json)
                rdb.sadd("lsh_keys", "lsh:{0}".format(tag))
        except Exception as e:
            print("ERROR: failed to save tag {0} to Redis".format(tag))


# Store duplicate candidates in Redis
def store_dup_cand_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for cand in rdd:
        cand_reformatted = (cand.q1_id, cand.q1_headline, cand.q2_id, cand.q2_headline, cand.q1_timestamp, cand.q2_timestamp)
        # Store by time
        rdb.zadd("dup_cand", cand.mh_js, cand_reformatted)


# Compares LSH signatures, MinHash signature, and find duplicate candidates
def find_dup_cands_within_tags():
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    # Fetch all tags from lsh_keys set
    for lsh_key in rdb.sscan_iter("lsh_keys", match="*", count=500):
        tag = lsh_key.replace("lsh:", "")
        tq_table_size = rdb.zcard("lsh:{0}".format(tag))

        # Fetch all news. ideally, sort the news by timestamp, and get within a range of timestamps
        tq = rdb.zrangebyscore("lsh:{0}".format(tag), "-inf", "+inf", withscores=False)
        if len(tq) < 2: continue
        if config.LOG_DEBUG: print("{0} news".format(len(tq)))
        tq_df = sql_context.read.json(sc.parallelize(tq))

        # find top similar set within a time window
        similar_sets_dict = find_similar_cands_lsh(tq_df)
        print(similar_sets_dict)

        # find_lsh_sim = udf(lambda x, y: util.common_bands_count(x, y), IntegerType())
        # lsh_sim_df = tq_df.alias("q1").join(tq_df.alias("q2"),
        #     [col("q1.timestamp") < col("q2.timestamp"),
        #     col("q2.timestamp") - col("q1.timestamp") < config.TIME_WINDOW]).select(
        #     col("q1.id").alias("q1_id"),
        #     col("q2.id").alias("q2_id"),
        #     col("q1.min_hash").alias("q1_min_hash"),
        #     col("q2.min_hash").alias("q2_min_hash"),
        #     col("q1.headline").alias("q1_headline"),
        #     col("q2.headline").alias("q2_headline"),
        #     col("q1.timestamp").alias("q1_timestamp"),
        #     col("q2.timestamp").alias("q2_timestamp"),
        #     find_lsh_sim("q1.lsh_hash", "q2.lsh_hash").alias("lsh_sim")
        # ).sort("q1_timestamp", "q2_timestamp")
        # # Duplicate candidates have a high enough LSH similarity count
        # lsh_cand_df = lsh_sim_df.filter(lsh_sim_df.lsh_sim >= config.LSH_SIMILARITY_BAND_COUNT)
        #
        # # Compare MinHash jaccard similarity scores for duplicate candidates
        # find_mh_js = udf(lambda x, y: util.jaccard_sim_score(x, y))
        # mh_cand_df = lsh_cand_df.withColumn("mh_js", find_mh_js(lsh_cand_df.q1_min_hash, lsh_cand_df.q2_min_hash))
        #
        # # Duplicate candidates need to have a high enough MinHash Jaccard similarity score
        # # dup_cand_df = mh_cand_df.filter(mh_cand_df.mh_js >= config.DUP_QUESTION_MIN_HASH_THRESHOLD)
        #
        # print("number of partitions: ", dup_cand_df.rdd.getNumPartitions())
        # dup_cand_df.foreachPartition(lambda rdd: store_dup_cand_redis(rdd))


# def find_similar_cands_lsh(df):
#     # find set of news ids which have at least one common lsh bucket
#     candidates_with_common_bucket = df.select(col('lsh_hash'), col('id')).rdd.flatMap(
#         lambda x: (((hash, band), [x[1]]) for band, hash in enumerate(x[0]))).reduceByKey(
#         lambda a, b: util._extend(a,b)).map(lambda x: tuple(x[1])).filter(lambda x: len(x)>1).distinct()
#     if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(candidates_with_common_bucket.first()))
#
#     rdd_dataset = candidates_with_common_bucket.map(lambda candiate_sets: get_jaccard_similarity(df, candidate_sets))
#     if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_dataset.first()))
#
#     similar_sets_dict = rdd_dataset.flatMap(lambda x: x.items()).reduceByKey(lambda acc, val: lsh.merge_result(acc, val)).collectAsMap()
#     if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(similar_sets_dict.first()))
#     return similar_sets_dict

# def get_jaccard_similarity(df, candidate_sets):
#     start_time = time.time()
#     rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
#     # Input whole df to calculate similar sets base on candidate_sets
#     # create base set and its similar sets in a dictionary.
#     # return = {base_set:(similar_set:jaccard_similarity, )}
#     _similar_dict = {}
#     if config.LOG_DEBUG: print('get_jaccard_similarity=>candidate_sets=%s'%(str(candidate_sets)))
#
#     # Generate combination for each set in candidate sets.
#     #candidate_df = df.filter('id' in candidate_sets).orderBy(df.timestamp.asc()).collect()
#     candidate_df = [df.filter(df.id == cand) for cand in candidate_sets]
#     if config.LOG_DEBUG: print('get_jaccard_similarity=>candidate_df=%s'%(str(candidate_df)))
#
#     for _b_set, _s_set in itertools.permutations(candidate_df,2):
#         _similar_dict[_b_set['id']] = []
#         if _b_set['timestamp'] < _s_set['timestamp'] or _b_set['timestamp'] > (_s_set['timestamp'] + config.TIME_WINDOW):
#             continue
#
#         #calculate jaccard similarity and update redis cache
#         jaccard_sim_token = 'jaccard_sim:{}:{}'.format(_b_set['id'], _s_set['id'])
#         if rdb.hexists(jaccard_sim_token):
#             _jaccard_similarity = rdb.hget(jaccard_sim_token)
#         else:
#             _jaccard_similarity = util.jaccard_sim_score(_b_set['min_hash'], _s_set['min_hash'])
#             rdb.hset(jaccard_sim_token, _jaccard_similarity)
#
#         # Store the result and get top NUM_OF_MOST_SIMILAR_SET similar sets
#         if _jaccard_similarity > config.DUP_QUESTION_MIN_HASH_THRESHOLD:
#             _similar_dict[_b_set['id']].append([_jaccard_similarity, _s_set['id']])
#
#     # filter and select top similar set.
#     _similar_dict = dict( [(k,sorted(v, key=lambda x: (x[0],-int(x[1][1:])), reverse=True)[:config.NUM_OF_MOST_SIMILAR_SET])
#                         for k,v in _similar_dict.items() if len(v)>0])
#
#     if DEBUG: print('get_jaccard_similarity=> _similar_dict=%s'%(_similar_dict))
#     end_time = time.time()
#     print("get_jaccard_similarity run time (seconds): {0} seconds".format(end_time - start_time))
#     return _similar_dict


def find_similar_cands_lsh(df):
    # find set of news ids which have at least one common lsh bucket
    if config.LOG_DEBUG: df.printSchema()
    rdd_cands_with_common_bucket = df.select(col('id'), col('min_hash'), col('headline'), col('timestamp'), col('lsh_hash')).rdd.flatMap(
        lambda x: (((hash, band), [(x[0], x[1], x[2], x[3])]) for band, hash in enumerate(x[4]))).reduceByKey(
        lambda a, b: util.custom_extend(a,b)).filter(lambda x: len(x[1])>1).map(lambda x: tuple(x[1]))
    #if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_cands_with_common_bucket.collect()))

    rdd_dataset = rdd_cands_with_common_bucket.map(lambda candidate_set: get_jaccard_similarity(candidate_set))
    #if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_dataset.first()))

    def _merge_result(acc_list, value_list):
        # Remove redundant similar sets from each partitions
        for _v in value_list:
            if _v not in acc_list:
                acc_list.append(_v)
            else: pass
            if config.LOG_DEBUG > 1: print('LSH.get_merge_result=> _final_dict=%s'%(_final_dict))
        return acc_list

    rdd_similar_sets_dict = rdd_dataset.flatMap(lambda x: x.items()).reduceByKey(lambda acc, val: _merge_result(acc, val)).collectAsMap()
    if config.LOG_DEBUG: print("find_similar_cands_lsh ==> {}".format(rdd_similar_sets_dict.collect()))
    return rdd_similar_sets_dict


def get_jaccard_similarity(candidate_set):
    """
    Input whole df to calculate similar sets base on candidate_set,
    create base set and its similar sets in a dictionary.
    return = {base_set:(similar_set:jaccard_similarity, )}
    """
    start_time = time.time()
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    _similar_dict = {}
    if config.LOG_DEBUG: print('get_jaccard_similarity=>candidate_set=%s'%(str(candidate_set)))

    for _b_set, _s_set in itertools.permutations(candidate_set,2):
        _similar_dict[(_b_set[0],_b_set[2],_b_set[3])] = []
        if _b_set[3] < _s_set[3] or _b_set[3] > (_s_set[3] + config.TIME_WINDOW):
            continue

        #calculate jaccard similarity and update redis cache
        jaccard_sim_token = 'jaccard_sim:{}:{}'.format(_b_set[0], _s_set[0])
        _jaccard_similarity = rdb.get(jaccard_sim_token)
        if _jaccard_similarity is None:
            _jaccard_similarity = util.jaccard_sim_score(_b_set[1], _s_set[1])
            rdb.set(jaccard_sim_token, _jaccard_similarity)

        # Store the result and get top NUM_OF_MOST_SIMILAR_SET similar sets
        if _jaccard_similarity > config.DUP_QUESTION_MIN_HASH_THRESHOLD:
            _similar_dict[(_b_set[0],_b_set[2],_b_set[3])].append([_jaccard_similarity, (_s_set[0],_s_set[2],_s_set[3]) ])

    # filter and select top similar set.
    _similar_dict = dict( [(k,sorted(v, key=lambda x: (x[0],-x[1][2]), reverse=True)[:config.NUM_OF_MOST_SIMILAR_SET])
                        for k,v in _similar_dict.items() if len(v)>0])
    if config.LOG_DEBUG: print('get_jaccard_similarity=> _similar_dict=%s'%(_similar_dict))

    end_time = time.time()
    print("get_jaccard_similarity run time (seconds): {0} seconds".format(end_time - start_time))
    return _similar_dict






def main():
    spark_conf = SparkConf().setAppName("Spark CustomMinHashLSH").set("spark.cores.max", "30")

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
    #df = util.read_all_json_from_bucket(sql_context, config.S3_BUCKET_BATCH_PREPROCESSED) # load historical data
    #mh, lsh = load_mh_lsh()
    #compute_minhash_lsh(df, mh, lsh) # Compute MinHash/LSH hashes for historical news

    # Compute pairwise LSH similarities for news within tags
    if (config.LOG_DEBUG): print("[BATCH]: Fetching questions,comparing LSH and MinHash, uploading duplicate candidates back to Redis...")
    find_dup_cands_within_tags()

    #candidate_sets = custom_lsh.find_similar_cands(df)
    #print('candiate_sets: {}'.format(candidate_sets))
    #candidate_sets.foreachPartition(lambda rdd: store_dup_cand_redis(rdd))

    end_time = time.time()
    print("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time))


if(__name__ == "__main__"):
    main()




"""
# understanding LSH parameters:
# https://towardsdatascience.com/understanding-locality-sensitive-hashing-49f6d1f6134
# https://santhoshhari.github.io/Locality-Sensitive-Hashing/
# LSH at UBER: https://eng.uber.com/lsh/
def run_minhash_lsh():
    df = util.read_all_json_from_bucket(sql_context, config.S3_BUCKET_BATCH_PREPROCESSED)
    if config.LOG_DEBUG: print(df.first())
    #  Create and save MinHash and LSH if not exist or load them from file
    if(not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = MinHash(config.MIN_HASH_K_VALUE)
        lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    # Compute MinHash/LSH hashes for every question
    if (config.LOG_DEBUG): print("[BATCH]: Calculating MinHash hashes and LSH hashes...")
    compute_minhash_lsh(df, mh, lsh)

    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print("[BATCH]: Fetching questions, comparing LSH and MinHash, uploading duplicate candidates back to Redis...")
    find_dup_cands_within_tags()


def compute_minhash_lsh(mh, lsh, df):
    # Compute MinHash/LSH hashes for input questions
    if (config.LOG_DEBUG): print("[BATCH]: Calculating MinHash hashes and LSH hashes...")
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    df.foreachPartition(store_lsh_redis)
    return df
    # # Compute pairwise LSH similarities for questions within tags
    # if (config.LOG_DEBUG): print("[BATCH]: Fetching questions, comparing LSH and MinHash, uploading duplicate candidates back to Redis...")
    # find_dup_cands_within_tags(mh, lsh)
"""
