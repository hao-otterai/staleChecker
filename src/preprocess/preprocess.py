import sys
import os
import re
import time

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, concat, col, lit, unix_timestamp

from pyspark.ml.feature import StopWordsRemover, Tokenizer
#from pyspark.ml.feature import HashingTF, IDF, VectorAssembler

import nltk
#nltk.download("wordnet") # only need to download once
from nltk.stem import WordNetLemmatizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

import redis

#import com.databricks.spark.xml
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.databricks.spark.xml pyspark-shell"

# Removes code snippets and other irregular sections from question body, returns cleaned string
def filter_body(body):
    remove_code = re.sub('<[^>]+>', "", body)
    remove_punctuation = re.sub(r"[^\w\s]", "", remove_code)
    remove_numerical = re.sub(r"[-?0-9]+", "", remove_punctuation)
    remove_spaces = remove_numerical.replace("\n", "")
    return remove_spaces.encode('ascii', 'ignore')


# Stems words
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return tuple(stems)


# Create 2 gram shingles from text body
def get_two_gram_shingles(tokens):
    return [(tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)]

# Create 3 gram shingles from text body
def get_tri_gram_shingles(tokens):
    return [(tokens[i], tokens[i + 1], tokens[i + 2]) for i in range(len(tokens) - 2)]

def generate_tag(input_string):
    return input_string.replace('/','_').split(";") if len(input_string)>0 else ['<UNS>']

def store_preprocessed_redis(iterator):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for news in iterator:
        #token = "preprocessed:{0}".format(news.id)
        save_content = {"headline": news.headline, "body": news.body, "timestamp": news.timestamp} #news.text_body_stemmed
        if config.LOG_DEBUG: print(save_content['headline'])
        #try:
        #rdb.zadd("newsId", news.timestamp, news.id)
        rdb.sadd("newsId", "{}:{}".format(news.id, news.timestamp))
        rdb.hmset("news:{}".format(news.id), save_content)
        #except Exception as e:
        #    print("ERROR: failed to save news id:{0} to Redis".format(news.id))



def df_preprocess_func(df):

    # Clean body
    clean_body = udf(lambda body: filter_body(body), StringType())
    df_cleaned = df.withColumn("cleaned_body", clean_body("body"))

    # generate tags based on company, industry, and market
    tag_generator = udf(lambda input_string: generate_tag(input_string), ArrayType(StringType()))
    df_tagged = df_cleaned.withColumn( "tag_company",  tag_generator("company"))

    # Concat cleaned body and headline to form vector
    df_textbody = df_tagged.withColumn("text_body", concat(col("headline"), lit(" "), col("cleaned_body")))

    # Tokenize text
    tokenizer = Tokenizer(inputCol="text_body", outputCol="text_body_tokenized")
    df_tokenized = tokenizer.transform(df_textbody)

    # Remove stop words
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    df_stopword = stop_words_remover.transform(df_tokenized)

    # Stem words
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    df_stemmed0 = df_stopword.withColumn("text_body_stemmed0", stem("text_body_stop_words_removed"))

    # remove words shorter than certain length, 2 for instance
    final_len_filter = udf(lambda tokens: [tok for tok in tokens if len(tok)>1], ArrayType(StringType()))
    df_stemmed = df_stemmed0.withColumn("text_body_stemmed", final_len_filter("text_body_stemmed0"))

    # Timestamp
    final_data = df_stemmed.withColumn("timestamp",unix_timestamp("display_date", "yyyyMMdd'T'HHmmss.SSS'Z'"))

    return final_data

    # Shingle resulting body
    # if (config.LOG_DEBUG): print("[PROCESSING] Shingling resulting text body...")
    # shingle = udf(lambda tokens: get_two_gram_shingles(tokens), ArrayType(ArrayType(StringType())))
    # shingled_data = stemmed_data.withColumn("text_body_shingled", shingle("text_body_stemmed"))

    # ### get TF-IDF vector
    # if (config.USE_TF_IN_PREPROCESSING):
    #     # Vectorize so we can fit to MinHashLSH model
    #     #htf = HashingTF(inputCol="text_body_stemmed", outputCol="raw_features", numFeatures=1000)
    #     htf = HashingTF(inputCol="text_body_stemmed", outputCol="raw_features")
    #     htf_df = htf.transform(final_data)
    #
    #     if (config.USE_TFIDF): # under maintenance
    #         idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq = config.MIN_DOC_FREQ)
    #         idfModel = idf.fit(htf_df)
    #         tfidf = idfModel.transform(featurizedData)
    #         vectorizer = VectorAssembler(inputCols=["features"], outputCol="text_body_vectorized")
    #         vdf = vectorizer.transform(htf_df)
    #     else:
    #         vectorizer = VectorAssembler(inputCols=["raw_features"], outputCol="text_body_vectorized")
    #         vdf = vectorizer.transform(htf_df)
    #     final_data = vdf
    #     final_output_fields += ", text_body_tokenized "



# Preprocess a data file and upload it
def preprocess_file(bucket_name, file_name):
    global sql_context

    df_raw = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name))
    if (config.LOG_DEBUG):
        df_raw.printSchema()

    final_data = df_preprocess_func(df_raw)
    # Extract data that we want
    final_data.registerTempTable("final_data")

    final_output_fields = "id, headline, body, text_body, text_body_stemmed, tag_company, source, hot, display_date, timestamp, djn_urgency"
    # tag_industry, tag_market,
    df_preprocessed =  sql_context.sql( "SELECT {} from final_data".format(final_output_fields) )

    if (config.LOG_DEBUG):
        df_preprocessed.printSchema()
        print(df_preprocessed.first())

    # write to Redis
    if config.LOG_DEBUG: print("store preprocessed news")
    df_preprocessed.foreachPartition(store_preprocessed_redis)

    # Write to AWS
    if config.LOG_DEBUG: print("[UPLOAD]: Writing preprocessed data to AWS...")
    util.write_json_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, file_name, df_preprocessed)


def main():
    spark_conf = SparkConf().setAppName("news preprocesser").set("spark.cores.max", "30")
    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    bucket = util.get_bucket(config.S3_BUCKET_BATCH_RAW)
    for csv_obj in bucket.objects.all():
        preprocess_file(config.S3_BUCKET_BATCH_RAW, csv_obj.key)
        print("Finished preprocessing file s3a://{0}/{1}".format(config.S3_BUCKET_BATCH_RAW, csv_obj.key))

    end_time = time.time()
    print("Preprocessing run time (seconds): {0}".format(end_time - start_time))


if(__name__ == "__main__"):
    main()
