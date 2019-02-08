import os
import sys
import smart_open
import kafka
import time
import threading
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

class Producer(threading.Thread):
    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=config.KAFKA_SERVERS,
                     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version=(0, 10))

        # bucket = util.get_bucket(config.S3_BUCKET_STREAM)
        # for json_obj in bucket.objects.all():
        #     json_file = "s3://{0}/{1}".format(config.S3_BUCKET_STREAM, json_obj.key)
        #     for json_bytes in smart_open.smart_open(json_file):
        #         for line in json.loads(json_bytes):
        #             if config.LOG_DEBUG: print(line)
        #             # time.sleep(config.KAFKA_PRODUCER_RATE)
        #             producer.send(config.KAFKA_TOPIC, line)
        i = 0
        while True:
            line = "hello world {}".format(i)
            if config.LOG_DEBUG: print(line)
            producer.send(config.KAFKA_TOPIC, line)
            time.sleep(0.5)
            i += 1

def main():
    producer = Producer()
    producer.start()
    print("Starting Kafka Producer: Ingesting at {0} events per second...".format(1.0 / (config.KAFKA_PRODUCER_RATE)))


if __name__ == "__main__":
    main()
