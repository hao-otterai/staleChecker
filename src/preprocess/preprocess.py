import sys
import os
import re
import time

from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.ml.feature import HashingTF, IDF, VectorAssembler

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, concat, col, lit

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

#import com.databricks.spark.xml

import nltk
nltk.download("wordnet")
from nltk.stem import WordNetLemmatizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

from pyspark.sql.functions import unix_timestamp



# Stems words
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return tuple(stems)


# Removes code snippets and other irregular sections from question body, returns cleaned string
def filter_body(body):
    remove_code = re.sub('<[^>]+>', '', body)
    remove_punctuation = re.sub(r"[^\w\s]", " ", remove_code)
    remove_numerical = re.sub(r"[-?0-9]+", " ", remove_punctuation)
    remove_spaces = remove_numerical.replace("\n", " ")
    return remove_spaces.encode('ascii', 'ignore')


# Create 2 gram shingles from text body
def get_two_gram_shingles(tokens):
    return [(tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)]

# Create 3 gram shingles from text body
def get_tri_gram_shingles(tokens):
    return [(tokens[i], tokens[i + 1], tokens[i + 2]) for i in range(len(tokens) - 2)]

def generate_tag(input_string):
    return input_string.replace('/','_').split(";") if len(input_string)>0 else ['<UNS>']

# Preprocess a data file and upload it
def preprocess_file(bucket_name, file_name):

    final_output_fields = "id, headline, body, text_body, text_body_stemmed, tag_company, \
    tag_industry, tag_market, source, hot, display_date, display_timestamp, djn_urgency"

    raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name))
    # raw_data = sc.textFile("s3a://{0}/{1}".format(bucket_name, file_name))
    # header = raw_data.first()
    # raw_data = raw_data.filter(lambda line: line != header)
    # #raw_data.take(10)
    # raw_data = raw_data.map(lambda k: k.split(",")).toDF(header.split(","))
    #raw_data.show()
    if (config.LOG_DEBUG):
        print('Schema of raw input data')
        raw_data.printSchema()

    # Clean question body
    if(config.LOG_DEBUG): print("[PROCESSING]: Cleaning headline and body...")
    clean_body = udf(lambda body: filter_body(body), StringType())
    partially_cleaned_data = raw_data.withColumn("cleaned_body", clean_body("body"))

    # generate tags based on company, industry, and market
    if (config.LOG_DEBUG): print("[PROCESSING]: Generating news tags based on industry, market and company...")
    tag_generator = udf(lambda input_string: generate_tag(input_string), ArrayType(StringType()))
    partially_cleaned_data = partially_cleaned_data.withColumn( "tag_company",  tag_generator("company"))
    partially_cleaned_data = partially_cleaned_data.withColumn( "tag_industry", tag_generator("industry"))
    partially_cleaned_data = partially_cleaned_data.withColumn( "tag_market",   tag_generator("market"))


    # Concat cleaned question body and question title to form question vector
    if (config.LOG_DEBUG): print("[PROCESSING]: Concating headline and body...")
    data = partially_cleaned_data.withColumn("text_body", concat(col("headline"), lit(" "), col("cleaned_body")))

    # Tokenize question title
    if (config.LOG_DEBUG): print("[PROCESSING]: Tokenizing text vector...")
    tokenizer = Tokenizer(inputCol="text_body", outputCol="text_body_tokenized")
    tokenized_data = tokenizer.transform(data)

    # Remove stop words
    if (config.LOG_DEBUG): print("[PROCESSING]: Removing stop words...")
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    stop_words_removed_data = stop_words_remover.transform(tokenized_data)

    # Stem words
    if (config.LOG_DEBUG): print("[PROCESSING]: Stemming tokenized vector...")
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    stemmed_data = stop_words_removed_data.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))
    final_data = stemmed_data
    # Shingle resulting body
    # if (config.LOG_DEBUG): print("[PROCESSING] Shingling resulting text body...")
    # shingle = udf(lambda tokens: get_two_gram_shingles(tokens), ArrayType(ArrayType(StringType())))
    # shingled_data = stemmed_data.withColumn("text_body_shingled", shingle("text_body_stemmed"))


    ### get TF-IDF vector
    if (config.USE_TF_IN_PREPROCESSING):
        # Vectorize so we can fit to MinHashLSH model
        #htf = HashingTF(inputCol="text_body_stemmed", outputCol="raw_features", numFeatures=1000)
        htf = HashingTF(inputCol="text_body_stemmed", outputCol="raw_features")
        htf_df = htf.transform(final_data)

        if (config.USE_TFIDF): # under maintenance
            idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq = config.MIN_DOC_FREQ)
            idfModel = idf.fit(htf_df)
            tfidf = idfModel.transform(featurizedData)
            vectorizer = VectorAssembler(inputCols=["features"], outputCol="text_body_vectorized")
            vdf = vectorizer.transform(htf_df)
        else:
            vectorizer = VectorAssembler(inputCols=["raw_features"], outputCol="text_body_vectorized")
            vdf = vectorizer.transform(htf_df)
        final_data = vdf
        final_output_fields += ", text_body_tokenized "

    # timestamp
    if (config.LOG_DEBUG): print("[PROCESSING]: Formatting unix_timestamp ...")
    # final_data = stemmed_data.withColumn("display_timestamp",unix_timestamp("display_date", "yyyyMMdd'T'HHmmss.SSS'Z'").cast('timestamp'))
    final_data = final_data.withColumn("display_timestamp",unix_timestamp("display_date", "yyyyMMdd'T'HHmmss.SSS'Z'"))


    # Extract data that we want
    final_data.registerTempTable("final_data")
    preprocessed_data = sql_context.sql( "SELECT {} from final_data".format(final_output_fields) )

    if (config.LOG_DEBUG):
        print('Schema of transformed input data')
        final_data.printSchema()
        print('Fields to output for final_data:', final_output_fields)
        print("[UPLOAD]: Writing preprocessed data to AWS...")

    # Write to AWS
    util.write_json_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, file_name, preprocessed_data)


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
