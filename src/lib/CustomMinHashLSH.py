import sys
import os
import time
import json
import itertools
import mmh3
import pickle  # Need to dump tuple to bytes so mmh3 can hash it
import numpy as np

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
import redis

from pyspark.sql.types import IntegerType, ArrayType, StringType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util


class CustomMinHashLSH(object):

    def __init__(self, ):
        self.conf = SparkConf.setAppName('customMinHashLSH')
        self.sc = SparkContext.getOrCreate(conf=self.conf)
        self.sc.setLogLevel("ERROR")
        self.sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
        self.sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")
        self.sql_context = SQLContext(sc)

        self.jaccard_similarity = {}
        self.init_mh_lsh()


    def init_mh_lsh(self,):
        #  Create and save MinHash and LSH if not exist or load them from file
        if(not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
            self.mh = MinHash(config.MIN_HASH_K_VALUE)
            self.lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
            util.save_pickle_file(self.mh, config.MIN_HASH_PICKLE)
            util.save_pickle_file(self.lsh, config.LSH_PICKLE)
        else:
            self.mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
            self.lsh = util.load_pickle_file(config.LSH_PICKLE)


    def compute_minhash_lsh(self, df):
        # Compute MinHash/LSH hashes for input questions
        if (config.LOG_DEBUG): print("[BATCH]: Calculating MinHash hashes and LSH hashes...")
        calc_min_hash = udf(lambda x: list(map(lambda x: int(x), self.mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
        calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), self.lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

        df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
        df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

        return df
        # df.foreachPartition(store_lsh_redis)
        #
        # # Compute pairwise LSH similarities for questions within tags
        # if (config.LOG_DEBUG): print("[BATCH]: Fetching questions, comparing LSH and MinHash, uploading duplicate candidates back to Redis...")
        # find_dup_cands_within_tags(mh, lsh)



    def get_jaccard_similarity(self, df, candidate_sets):
        # Input whole df to calculate similar sets base on candidate_sets
        # create base set and its similar sets in a dictionary.
        # return = {base_set:(similar_set:jaccard_similarity, )}
        _similar_dict = {}
        if config.LOG_DEBUG: print('get_jaccard_similarity=>candidate_sets=%s'%(str(candidate_sets)))

        # Generate combination for each set in candidate sets.
        candidate_df = df.filter('id' in candidate_sets).orderBy(df.timestamp.asc()).collect()
        if config.LOG_DEBUG: print('get_jaccard_similarity=>candidate_df=%s'%(str(candidate_df)))

        for _b_set, _s_set in itertools.permutations(candidate_df,2):
            _similar_dict[_b_set['id']] = []
            if _b_set['timestamp'] < _s_set['timestamp'] or _b_set['timestamp'] > _s_set['timestamp'] + config.TIME_WINDOW:
                continue

            #calculate jaccard similarity.
            if (_b_set['id'], _s_set['id']) in self.jaccard_similarity: #in local cache
                _jaccard_similarity = self.jaccard_similarity[(_b_set['id'], _s_set['id'])]
            else:   #calculate jaccard similarity
                _jaccard_similarity = util.jaccard_sim_score(_b_set['min_hash'], _s_set['min_hash'])
                # Put calculation results into local cache
                self.jaccard_similarity[(_b_set['id'], _s_set['id'])] = _jaccard_similarity

            # Store the result and get top NUM_OF_MOST_SIMILAR_SET similar sets
            if _jaccard_similarity > config.DUP_QUESTION_MIN_HASH_THRESHOLD:
                _similar_dict[_b_set['id']].append([_jaccard_similarity, _s_set['id']])

        _similar_dict = dict( [(k,sorted(v, key=lambda x: (x[0],-int(x[1][1:])), reverse=True)[:config.NUM_OF_MOST_SIMILAR_SET])
                            for k,v in _similar_dict.items() if len(v)>0])

        if DEBUG: print('get_jaccard_similarity=> _similar_dict=%s'%(_similar_dict))
        return _similar_dict


    def find_similar_cands(self, df):
        # broadcase the data

        # find set of news ids which have at least one common lsh bucket
        _candidates_with_common_bucket = df.select(col('lsh_hash'), col('id')).rdd.flatMap(
            lambda x: (((hash, band), [x[1]]) for band, hash in enumerate(x[0]))).reduceByKey(
            lambda a, b: util._extend(a,b)).map(lambda x: x[1]).filter(lambda x: len(x)>1).distinct()
        # _candidates_with_common_bucket = df.select(col('id'), col('headline'), col('min_hash'), col('lsh_hash')).rdd.flatMap(
        #     lambda x: (((hash, band), [(x[0], x[1], x[2])]) for band, hash in enumerate(x[3]))).reduceByKey(
        #     lambda a, b: _extend(a,b)).map(lambda x: x[1]).filter(lambda x: len(x)>1).distinct()

        rdd_dataset = _candidates_with_common_bucket.map(lambda candiate_sets: self.get_jaccard_similarity(df, candidate_sets))
        _similar_sets_dict = rdd_dataset.flatMap(lambda x: x.items()).reduceByKey(lambda acc, val: lsh.merge_result(acc, val)).collectAsMap()

        return _similar_sets_dict




class MinHash(object):
    def __init__(self, k, random_seed=50):
        self._k = k
        self._random_seed = random_seed
        self._masks = (np.random.RandomState(seed=self._random_seed).randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, self._k))

    def update_min_hash_signature(self, word, min_hash_signature):
        root_hash = mmh3.hash64(word.encode("ascii", "ignore"))[0]
        # root_hash = mmh3.hash64(pickle.dumps(word))[0]  # For MinHashing shingles
        word_hashes = np.bitwise_xor(self._masks, root_hash)  # XOR root hash with k randomly generated integers to simulate k hash functions, can add bitroll if there's time
        min_hash_signature = np.minimum(min_hash_signature, word_hashes)
        return min_hash_signature

    def calc_min_hash_signature(self, tokens):
        min_hash_signature = np.empty(self._k, dtype=np.int64)
        min_hash_signature.fill(np.iinfo(np.int64).max)
        for token in tokens:
            min_hash_signature = self.update_min_hash_signature(token, min_hash_signature)
        return min_hash_signature



class LSH(object):
    def __init__(self, num_bands, band_width, num_buckets=1000, random_seed=50):
        self._num_bands = num_bands
        self._band_width = band_width
        self._num_buckets = num_buckets

    def find_lsh_buckets(self, hash_signature):
        bands = [tuple(hash_signature[i:i + self._band_width]) for i in range(0, len(hash_signature), self._band_width)]
        lsh_hashes = [(mmh3.hash64(pickle.dumps(row))[0] % self._num_buckets) for row in bands]
        return lsh_hashes

    def common_bands_count(self, a, b):
        return len(set(a) & set(b))

    def common_bands_ratio(self, a, b):
        return len(set(a) & set(b)) / (1.0 * self._num_bands)
