import os
import sys
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

        file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "2001_sample_10M_stream.json")
        with open(file_dir) as f: json_file = json.load(f)
        for line in json_file:
            if config.LOG_DEBUG: print(line)
            producer.send(config.KAFKA_TOPIC, line)

def main():
    producer = Producer()
    producer.start()
    print("Starting Kafka Producer...")

if __name__ == "__main__":
    main()
