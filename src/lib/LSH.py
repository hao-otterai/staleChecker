from __future__ import print_function
from __future__ import division
# Import the necessary Spark library classes, as well as sys
from pyspark import SparkConf, SparkContext, StorageLevel
import collections
from itertools import combinations
from datetime import datetime
import sys
import math

from util import setOutputData, getInputData

__all__ = []
__version__ = 1.2
__date__ = '2017-03-08'
__updated__ = '2017-03-13'

APP_NAME = 'LSH'
SPLITTER = ',' #Data separate symbol
BANDS = 5
ROWS = 4 # 4 values in each Band
NUM_OF_MOST_SIMILAR_SET = 5 #Top 5 similar sets will be selected for each set

USE_UNICODE = False
DEBUG = 0 # Level:0=No log, :1=Normal, :2=Detail
PRINT_TIME = True

INPUT_FILE = 'input.txt'
#OUTPUT_FILE = 'output.txt' # OUTPUT_FILE COULD BE 'OUTPUT_FILE = None' for console or file name (e.g. 'OUTPUT_FILE = 'output.txt') for file.'
OUTPUT_FILE = None # OUTPUT_FILE COULD BE 'OUTPUT_FILE = None' for console or file name (e.g. 'OUTPUT_FILE = 'output.txt') for file.'


class LSH(object):
    '''
    This class implements Locality-Sensitive Hash (LSH) algorithm.
    The implementation partitions data by into sub-dataset for parallel process.

    a. Data list format: csv file convert into below format:
      {set1:[item7, item2, item3, ..., itemN], set2:[...],..., setM:[itemI, itemJ, ..., itemZ]}

    b. Procedures:
        1. According to band and row size, create row number list
            -[0,1,2,3...R]
        2.
            a. Parallel dataset into several partitions for minhash calculation.
            b. Distribute dataset into all partitions for jaccard similarity calculation.
        3. Calculating Minhash value as signature for each set in each band
            - Get minimun hash value from items in the set by each hash function.
            - Store the results in each band as below:
                - take signatures of one band for each set as key, the set as value
                - [((band_B), ((minhash_1), (minhash_2)...(minhash_rows)), setM]
        4. Find the similar sets which have identical signatures (minhash values) for each item in one of bands.
            - Find similar sets in each band based on identical band_no + signatures as key of each set
            - Collect similar sets from each band.
                - [Band_A(setI, setN, setK, setN),..., Band_b(...)]
        5. Calculating jaccard similarity for each set in each band. Select similar sets which fulfill threshold criteria by each band.
            -[(setI,((setN, jsN), (setK, jsK), (setQ, jsQ)),..., (setK, ((setI, jsI),(setM, jsM),(setN, jsN)))]
        6. Merge all similar sets from each band and remove redundant sets.
        7. Output the result.
            - select similar sets which fulfill threshold criteria for each set from all bands.
            - Sort those set by their set ID (tiebreaker by smaller ID first).


    '''
    bands = None
    rows = None
    hash_alg = None
    jaccard_similarity = {} # {(set1, set2): jaccard_similarity, ...}
    def __init__(self, data_set='', bands=BANDS, rows = ROWS, hash_function=customized_hash):
        '''
        Constructor
        1. Input file contains item sets to be calculated.
            CSV data format:
                set1, item7, item2, item3, ..., itemN
                set2, item1, item 5, item8, ..., itemK
                ...
                setM, itemI, itemJ, ...., itemZ
        2. You can configure your own hash function for LSH.
            customized_hash(data, seed) should provide a hash value according data, and an assigned seed value will generate different hash value for same data.
        '''
        self.conf = SparkConf().setAppName(APP_NAME).setMaster("local[*]")
        # Create a context for the job.
        self.sc = SparkContext(conf=self.conf)

        LSH.hash_alg = MinHash(hash_function)
        LSH.band = bands
        LSH.rows = rows

        self.dataset = data_set
        self.rdd_dataset = self.sc.parallelize(self.dataset)
        self.b_dataset = self.sc.broadcast(self.dataset)
        self.row_list = self.get_rows(bands, rows)
        self.b_row_list = self.sc.broadcast(self.row_list)


        self.jaccard_similary_dict = {}
        if DEBUG: print('rdd_bands=%s, partitions=%d'%(self.rdd_band, self.rdd_band.getNumPartitions()))

    @staticmethod
    def get_set_signatures(set, seed_list):
        _result = []
        _signatures = []
        _band = 0

        if DEBUG: print('LSH.get_set_signatures=>seed_list=%s'%(seed_list))
        if DEBUG: print('LSH.get_set_signatures=>set=%s'%(str(set)))
        for seed in seed_list:
            if _band < math.floor(seed/LSH.rows) :
                _result.append((tuple(_signatures), set[0]))
                _signatures = [_band]
                _band += 1
            else: pass
            _signatures.append(LSH.hash_alg.get_value(set[1], seed)) #Get minhash signature for each row/seed
        _result.append((tuple(_signatures), set[0]))
        if DEBUG: print('Minhash results=%s'%(_result))
        return _result

    def get_rows(self, bands, rows):
        # Return get_bands=[0, 1, 2, 3, 4, 5, 6, 7...N]
        _rows = []

        for i in range(bands):
            for j in range(rows):
                _rows.append(i*rows+j)

        if DEBUG: print('LSH.get_rows=%s'%(_rows))
        return _rows


    def execute(self):
        _similar_sets_dict = {}
        _jaccard_similary_list = []
        _row_list = self.b_row_list.value
        _rdd_dataset = self.rdd_dataset
        _dataset = self.b_dataset.value
        if DEBUG: print ('LSH.execute=>_rdd_dataset =%s'%(str(_rdd_dataset)))

        _rdd_similar_set_candidate_list = _rdd_dataset.map(lambda x: LSH.get_set_signatures(x, _row_list)).flatMap(lambda x:
            ((x[i][0], x[i][1]) for i in range(len(x)))).groupByKey().map(lambda x: tuple(x[1])).filter(lambda x: len(x)>1).distinct()
        if DEBUG: print ('LSH.execute=>_rdd_similar_set_candidate_list =%s'%(_rdd_similar_set_candidate_list.collect()))

        rdd_dataset = _rdd_similar_set_candidate_list.map(lambda candidate_sets: LSH.get_jaccard_similarity(_dataset, candidate_sets))
        _similar_sets_dict = rdd_dataset.flatMap(lambda x: x.items()).reduceByKey(lambda acc, val: LSH.merge_result(acc, val)).collectAsMap()
        if DEBUG: print('LSH.execute=>_similar_sets_dict2=%s'%(_similar_sets_dict))
        return _similar_sets_dict

    @staticmethod
    def merge_result(acc_list, value_list):
        # Remove redundant similar sets from each partitions
        for _v in value_list:
            if _v not in acc_list:
                acc_list.append(_v)
            else: pass
            if DEBUG > 1: print('LSH.get_merge_result=> _final_dict=%s'%(_final_dict))
        return acc_list


    @staticmethod
    def get_jaccard_similarity(dataset, candidate_sets):
        # Input whole dataset to calculate similar sets base on candidate_sets
        # create base set and its similar sets in a dictionary.
        # candidate_sets= = (setI, setJ, setK) =('U2', 'U9', 'U10')
        # return = {base_set:(similar_set:jaccard_similarity, )}
        _similar_dict = {}
        _result = []
        _dataset_dict = dict(dataset)
        _set = None
        _similar_sets = None
        _total_set_list = []
        _counter = 0
        _jaccard_similarity = 0.0
        if DEBUG: print('LSH.get_jaccard_similarity=>candidate_sets=%s'%(str(candidate_sets)))
        if DEBUG: print('type(_dataset_dict)=%s, _dataset_dict = %s'%(type(_dataset_dict),_dataset_dict))

        # Generate combination for each set in candidate sets.
        for i in range(len(candidate_sets)):
            _b_set = candidate_sets[i] #base set
            _result = []
            for j in range(len(candidate_sets)):
                if i != j:
                    _s_set = candidate_sets[j] #similar set
                    _jaccard_similarity = 0.0
                    _total_set_list = []

                    #calculate jaccard similarity.
                    if tuple((_b_set, _s_set)) in LSH.jaccard_similarity: #in local cache
                        _jaccard_similarity = LSH.jaccard_similarity[(_b_set, _s_set)]
                    else:   #calculate jaccard similarity
                        _b_items = _dataset_dict[_b_set]
                        _s_items = _dataset_dict[_s_set]
                        _total_set_list.append(set(_b_items))
                        _total_set_list.append(set(_s_items))
                        _jaccard_similarity = float(len(set.intersection(*_total_set_list))/len(set.union(*_total_set_list)))
                        # Put calculation results into local cache
                        LSH.jaccard_similarity.update({tuple((_b_set, _s_set)):_jaccard_similarity})
                        LSH.jaccard_similarity.update({tuple((_s_set, _b_set)):_jaccard_similarity})
                    # Store the result and get top NUM_OF_MOST_SIMILAR_SET similar sets
                    _result.append([_jaccard_similarity, _s_set])
                    _result = sorted(_result, key=lambda x: (x[0],-int(x[1][1:])), reverse=True)[:NUM_OF_MOST_SIMILAR_SET]
                    if DEBUG > 1: print('sort by jaccard similarity _result=%s'%(_result))
                else: pass

            _similar_dict[_b_set]=_result
        if DEBUG: print('LSH.get_jaccard_similarity=> _similar_dict=%s'%(_similar_dict))
        return _similar_dict

if __name__ == "__main__":

    '''
        Main program.
            Read dataset from csv file.
            Data format:
             set1, item7, item2, item3, ..., itemN
             set2, item1, item 5, item8, ..., itemK
             ...
             setM, itemI, itemJ, ...., itemZ

        Data list format: csv file convert into below format.
        {set1:[item7, item2, item3, ..., itemN], set2:[...],..., setM:[itemI, itemJ, ..., itemZ]}
    '''
    if len(sys.argv) == 1 :
        print('Usage: %s, input_file.txt output_file.txt'%(sys.argv[0]))
        spark.stop()
    else:
        if len(sys.argv) > 1: input_file = sys.argv[1]
        else: input_file = INPUT_FILE
        if len(sys.argv) > 2: output_file = sys.argv[2]
        else: output_file = OUTPUT_FILE


    # Initial LSH object and read input file
    #
    if PRINT_TIME : print ('LSH=>Start=>%s'%(str(datetime.now())))
    data_list = getInputData(input_file)
    jaccard_similarity_dict = {}

    lsh = LSH(data_list, BANDS, ROWS)
    jaccard_similarity_dict = lsh.execute()
    setOutputData(output_file, jaccard_similarity_dict)
    if PRINT_TIME : print ('LSH=>Finish=>%s'%(str(datetime.now())))
