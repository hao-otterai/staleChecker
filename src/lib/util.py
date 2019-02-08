import os
import sys
import boto3
import botocore
import time
import pickle

from functools import reduce
from pyspark.ml.feature import HashingTF, IDF

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config

global sql_context

def custom_extend(a,b): # both a, b are list
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
# def union_dfs(*dfs):
#     return reduce(DataFrame.unionAll, dfs)

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
