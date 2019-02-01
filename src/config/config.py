
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
KAFKA_SERVERS = ""  # ip:port for kafka brokers
KAFKA_PRODUCER_RATE = 0  # seconds
KAFKA_TOPIC = ""
KAFKA_CONSUMER_REFRESH = 0  # seconds


# Spark settings - streaming, batch
SPARK_STREAMING_MINI_BATCH_WINDOW = 0  # seconds


# Redis settings
REDIS_SERVER = ""


# MinHash, LSH parameters
MIN_HASH_K_VALUE = 0
LSH_NUM_BANDS = 0
LSH_BAND_WIDTH = 0
LSH_NUM_BUCKETS = 0
LSH_SIMILARITY_BAND_COUNT = 0  # Number of common bands needed for MinHash comparison


# Dump files to synchronize models across spark streaming/batch
MIN_HASH_PICKLE = SRC_PATH + "/lib/mh.pickle"
LSH_PICKLE = SRC_PATH + "/lib/lsh.pickle"


# Duplicate question settings
DUP_QUESTION_MIN_HASH_THRESHOLD = 0
DUP_QUESTION_MIN_TAG_SIZE = 0
DUP_QUESTION_IDENTIFY_THRESHOLD = 0
QUESTION_POPULARITY_THRESHOLD = 0
