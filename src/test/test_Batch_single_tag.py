import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_process")

import batchCustomMinHashLSH as batch


def main():
    spark_conf = SparkConf().setAppName("Test Batch Single Tag").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")
    global sql_context
    sql_context = SQLContext(sc)

    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # Fetch all tags from lsh_keys set
    tag = 'uns'
    if "lsh:{}".format(tag) not in rdb.smembers('lsh_keys'):
        print('Failed: Tag not in lsh_keys')
        return

    tq_table_size = rdb.zcard("lsh:{0}".format(tag))

    mh, lsh = batch.load_mh_lsh()

    batch.find_similar_cands_per_tag(tag, mh, lsh)


if(__name__ == "__main__"):
    main()
