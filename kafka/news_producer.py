import json
from smart_open import smart_open, s3_iter_bucket
from kafka import KafkaProducer
import time


KafKaConn = 'ec2-52-13-241-228.us-west-2.compute.amazonaws.com:9092'

S3BucketDir =  's3://my-bucket-dowjones/2018-05-28.nml'

def main():

    producer = KafkaProducer(bootstrap_servers=KafKaConn)

    print(int(time.time()))
    for line in smart_open(S3BucketDir):
        producer.send('queries', line)
        producer.flush()
        time.sleep(.01)

if __name__ == '__main__':
    main()
