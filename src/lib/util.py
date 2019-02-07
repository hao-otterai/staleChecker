import os
import sys
import boto3
import botocore
import time
import pickle

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.ml.feature import HashingTF, IDF

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config

global sql_context
''' General utility functions (not including database functions) used across multiple files '''


def _extend(a,b): # both a, b are list
    a.extend(b)
    return a

# Returns first common tag between two tag lists, may not be the main tag
def common_tag(x, y):
    x_tags = x.split("|")
    y_tags = y.split("|")
    intersect = list(set(x_tags) & set(y_tags))
    return "" if len(intersect) < 1 else intersect[0]

# Reads all JSON files from an AWS bucket
def read_all_json_from_bucket(sql_context, bucket_name):
    if(config.LOG_DEBUG): print("[BATCH]: Reading S3 files to master dataframe...")
    return sql_context.read.json("s3a://{0}/*/*.json*".format(bucket_name))

# Retrieves AWS bucket object
def get_bucket(bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            return None
        else:
            return s3.Bucket(bucket_name)

# Unions dataframes with same schema
def union_dfs(*dfs):
    return reduce(DataFrame.unionAll, dfs)

# Decorator for timing processess
def time_process(func, process_name):
    start_time = time.time()
    func()
    end_time = time.time()
    print("{0} run time (seconds): {1}".format(process_name, end_time - start_time))

# Wrappers for loading/saving pickle files
def load_pickle_file(filepath):
    if(os.path.isfile(filepath)):
        with open(filepath, "rb") as p:
            hs = pickle.load(p)
        return hs
    return None

def save_pickle_file(data, filename):
    with open(filename, "wb") as p:
        pickle.dump(data, p, protocol=pickle.HIGHEST_PROTOCOL)


def write_json_aws_s3(bucket_name, file_name, df):
    df.write.save("s3a://{0}/{1}".format(bucket_name, file_name), format="json", mode="overwrite")


def jaccard_sim_score(x, y):
    intersection = set(list(x)).intersection(set(list(y)))
    union = set(list(x)).union(set(list(y)))
    return len(intersection) / (len(union) * 1.0)

def sim_count(x, y):
    return len(set(list(x)).intersection(set(list(y))))



# calculate TF-IDF
def compute_tf_idf(documents, minDocFreq):
    hashingTF = HashingTF()
    tf = hashingTF.transform(documents)
    # While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    # First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    idf = IDF(minDocFreq=minDocFreq).fit(tf)
    tfidf = idf.transform(tf)
    return tfidf


#
# def getInputData(filename):
# # Get data from input file.
#     _data = []
#
#     try:
#         with open(filename, 'r') as _fp:
#             for _each_line in _fp:
#                 _row = _each_line.strip().split(SPLITTER)
#                 _data.append((_row[0],_row[1:]))
#             if DEBUG: print(_data)
#         _fp.close()
#         return _data
#     except IOError as _err:
#         if DEBUG:
#             print ('File error: ' + str (_err))
#         else :
#             pass
#         exit()
#
# def setOutputData(filename='', jaccard_similarity_dict={}):
#     # output results.
#     try:
#         if filename != None :
#             orig_stdout = sys.stdout
#             f = file(filename, 'w')
#             sys.stdout = f
#         else:
#             pass
#     ##########
#
#         if DEBUG: print('**jaccard_similarity_dict = %s'%(jaccard_similarity_dict))
#         _sorted_list = sorted(jaccard_similarity_dict.items(), key=lambda x: int(x[0][1:])) # x[0] return key, then get characters from second and convert to integer.
#         if DEBUG: print('**_sorted_list = %s'%(_sorted_list))
#         for _x in _sorted_list: # get base set
#             _count = 0
#             _b_set = _x[0]
#
#             if DEBUG: print('_x=%s'%(str(_x)))
#             _result = sorted(_x[1], key=lambda x: (x[0],-int(x[1][1:])), reverse=True)[:NUM_OF_MOST_SIMILAR_SET] #Get top NUM_OF_MOST_SIMILAR_SET similar sets
#             if DEBUG: print('sort by jaccard similarity _result=%s'%(_result))
#             _result = sorted(_result, key=lambda x: int(x[1][1:])) # sorted by set id
#             if DEBUG: print('sort by similarity set name _result=%s'%(_result))
#
#             print('%s:'%(str(_b_set)), end ='')
#             for _r in _result: # get data from similar sets.
#                 if _count == 0:
#                     _count += 1
#                     print('%s'%(str(_r[1])), end ='') # print sorted similar set
#                 else:
#                     print(',%s'%(str(_r[1])), end ='') # print sorted similar set
#             print(end='\n')
#     ###########
#         sys.stdout.flush()
#         if filename != None :
#             sys.stdout = orig_stdout
#             f.close()
#         else:
#             pass
#     except IOError as _err:
#         if (DEBUG == True):
#             print ('File error: ' + str (_err))
#         else :
#             pass
#         exit()
