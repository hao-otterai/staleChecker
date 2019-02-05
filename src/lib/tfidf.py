from pyspark.ml.feature import HashingTF, IDF


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
