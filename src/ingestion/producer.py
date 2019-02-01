import os
import sys
import smart_open
import kafka
import time
import threading
from termcolor import colored
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

class Producer(threading.Thread):
    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=config.KAFKA_SERVERS)

        bucket = util.get_bucket(config.S3_BUCKET_STREAM)

        for json_obj in bucket.objects.all():
            json_file = "s3://{0}/{1}".format(config.S3_BUCKET_STREAM, json_obj.key)
            for line in smart_open.smart_open(json_file):
                    if config.LOG_DEBUG: print(line)
                    time.sleep(config.KAFKA_PRODUCER_RATE)
                    producer.send(config.KAFKA_TOPIC, line)


def main():
    producer = Producer()
    producer.start()
    print(colored("Starting Kafka Producer: Ingesting at {0} events per second...".format(
                1.0 / (config.KAFKA_PRODUCER_RATE)), "green"))


if __name__ == "__main__":
    main()
