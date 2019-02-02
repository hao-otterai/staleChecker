from smart_open import smart_open, s3_iter_bucket
from kafka import KafkaProducer
import time



# def convert_xml_to_json(bucket_name, file_name):
#     df = sql_context.read.format("com.databricks.spark.xml").option(
#         "rowTag", "doc").load("s3a://{0}/{1}".format(bucket_name, file_name))
#     flattened = df.withColumn("pre", explode("djnml.body.text.pre"))
#     selectedData = flattened.select("_transmission-date",
#         "djnml.head.docdata.djn.djn-newswire.djn-mdata._hot",
#         "djnml.head.docdata.djn.djn-newswire.djn-urgency",
#         "djnml.head.docdata.djn.djn-newswire.djn-mdata._display-date",
#         "djnml.body.headline",
#         "djnml.body.text")
#
#     write_aws_s3(bucket_name, file_name=, selectedData)
#     #selectedData.show(3,false)
#     #output_file = file_name.replace('nml','.csv')
#     #selectedData.write.format("com.databricks.spark.csv").option(
#     #    "header", "true").mode("overwrite").save(output_file)
#
# def run_xml2json_conversion():
#     bucket = util.get_bucket(config.S3_BUCKET_BATCH_RAW)
#     for fileobj in bucket.objects.all():
#         convert_xml_to_json(config.S3_BUCKET_BATCH_RAW, fileobj.key)
#         print("Finished preprocessing file s3a://{0}/{1}".format(config.S3_BUCKET_BATCH_RAW, fileobj.key))



def main():

    for line in smart_open('s3://dowjones-bucket/sample_docs.xml'):
        print(line)
        time.sleep(.01)

if __name__ =='__main__':
    main()
