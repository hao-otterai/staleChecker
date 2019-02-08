import os
import sys
import smart_open
import kafka
import time
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

import json

class Producer(threading.Thread):
    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                        api_version=(0, 10, 1))

        line = "hello kafka!"
        for i in range(1000):
            if config.LOG_DEBUG: print(line)
            time.sleep(0.1)
            producer.send(config.KAFKA_TOPIC, line)


def main():
    producer = Producer()
    producer.start()
    if config.LOG_DEBUG: print('servers: {}'.format(config.KAFKA_SERVERS))
    print("Starting Kafka Producer: Ingesting at {0} events per second...".format(10))


if __name__ == "__main__":
    main()
