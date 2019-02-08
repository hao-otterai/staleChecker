import threading
import time
import kafka
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import config


class Consumer(threading.Thread):
    def run(self):
        consumer = kafka.KafkaConsumer(bootstrap_servers=['localhost:9092'])
        consumer.subscribe([config.KAFKA_TOPIC])

        if config.LOG_DEBUG:
            for message in consumer:
                print("{0}\n\n".format(message))


def main():
    consumer = Consumer()
    consumer.daemon = True
    while True:
        if not consumer.isAlive():
            print("Starting Kafka Consumer...")
            consumer.start()
        else:
            print("Listening for topic: {0}...".format(config.KAFKA_TOPIC))
            time.sleep(0.1)


if __name__ == "__main__":
    main()
