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

        i = 0
        while i<1000:
            line = "hello world {}".format(i)
            if config.LOG_DEBUG: print(line)
            producer.send(config.KAFKA_TOPIC, line)
            time.sleep(0.1)
            i += 1

def main():
    producer = Producer()
    producer.start()
    print("Starting Kafka Producer: Ingesting at {0} events per second...".format(1.0 / (config.KAFKA_PRODUCER_RATE)))


if __name__ == "__main__":
    main()
