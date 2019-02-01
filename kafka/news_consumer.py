from kafka import KafkaConsumer
import time
import requests
import json
import random
import rethinkdb as r

last_len = 0
my_list = ['REFUND', 'GIFT CARD', 'PAYMENT', 'DEALS', 'ORDER']
es_ip_list = ["54.202.162.168","54.187.80.212","34.209.43.108"]
kafka_list = ["52.34.235.156:9092","54.68.129.82:9092","52.36.60.60:9092"]


def main():

    consumer = KafkaConsumer(bootstrap_servers=kafka_list, group_id='my_favorite_group')
    consumer.subscribe(['yo2'])
    for message in consumer:
        # print message.value
        json_data = json.loads(message.value)
        ip_index = random.randint(0, len(es_ip_list)-1)
        res = requests.get("http://"+ es_ip_list[ip_index] +":9200/amazon/type/_percolate",data = '{"doc" : {"message" : \"'+ json_data["message"] + ' \"}}')
        d = res.json()
        # print "Elastic Response is: " + str(d)
        num = int(d["matches"][0]["_id"])
        # print "Belongs to " + my_list[num]+ " bucket"
        r.connect("ec2-34-208-199-46.us-west-2.compute.amazonaws.com", 28015).repl()
        table_name=""
        if " " in my_list[num]:
            for word in my_list[num].split(" "):
                table_name=table_name + word + "_"
            table_name=table_name.strip("_")
        else:
            table_name=my_list[num]
        ts = time.time()
        json_data["timestamp"] = ts - json_data["timestamp"]
        r.table(table_name).insert(json_data).run()

if __name__ == '__main__':
    main()
