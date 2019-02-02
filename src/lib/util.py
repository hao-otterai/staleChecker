import os
import sys
import boto3
import botocore
import time
import pickle

from functools import reduce

from pyspark.sql import DataFrame

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config

global sql_context
''' General utility functions (not including database functions) used across multiple files '''

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
