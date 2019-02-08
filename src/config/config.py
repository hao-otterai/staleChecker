
''' IMPORTANT: RENAME THIS FILE TO CONFIG.PY TO RUN '''

import os

# Program settings
SRC_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DEBUG = True


# Preprocessing settings
NUM_SYNONYMS = 0  # num closest synonyms to add to token vector


# AWS settings
S3_BUCKET_BATCH_PREPROCESSED = "my-bucket-dowjones-batch"
S3_BUCKET_BATCH_RAW = "my-bucket-dowjones-raw"
S3_BUCKET_STREAM = "my-bucket-dowjones-stream"

# Kafka settings
KAFKA_SERVERS = ["ec2-52-34-80-47.us-west-2.compute.amazonaws.com:9092",
                "ec2-35-161-10-145.us-west-2.compute.amazonaws.com:9092",
                "ec2-34-210-190-100.us-west-2.compute.amazonaws.com:9092"]  # ip:port for kafka brokers

KAFKA_PRODUCER_RATE = 0.05  # seconds
KAFKA_TOPIC = "dowjones"
KAFKA_CONSUMER_REFRESH = 0.05  # seconds


# Spark settings - streaming, batch
SPARK_STREAMING_MINI_BATCH_WINDOW = 0.05  # seconds


# Redis settings
REDIS_SERVER = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"


# MinHash, LSH parameters
MIN_HASH_K_VALUE = 200
LSH_NUM_BANDS = 40
LSH_BAND_WIDTH = 5
LSH_NUM_BUCKETS = 20
LSH_SIMILARITY_BAND_COUNT = 5  # Number of common bands needed for MinHash comparison


# Dump files to synchronize models across spark streaming/batch
MIN_HASH_PICKLE = SRC_PATH + "/lib/mh.pickle"
LSH_PICKLE = SRC_PATH + "/lib/lsh.pickle"


# Duplicate question settings
DUP_QUESTION_MIN_HASH_THRESHOLD = 0.6
DUP_QUESTION_MIN_TAG_SIZE = 0
DUP_QUESTION_IDENTIFY_THRESHOLD = 0.55
QUESTION_POPULARITY_THRESHOLD = 0
NUM_OF_MOST_SIMILAR_SET = 10

# TF-idf
USE_TF_IN_PREPROCESSING = False
USE_TFIDF = False # use TF-IDF when set to True, else use TF (no IDF)
MIN_DOC_FREQ = 2

# time window for news similarity search
TIME_WINDOW = 432000 #3600*24*3
