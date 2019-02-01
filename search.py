import sys
import json
import time
import os
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import rethinkdb as r
import hashlib


def search():

    # db connection
    conn = r.connect(host='ec2-54-202-215-9.us-west-2.compute.amazonaws.com', port='28015', db='alluvium')
    #conn = r.connect(host='10.0.0.11', port='28015', db='alluvium')

    # Listen to Kafka docs topic
    #c = '10.0.0.14:9092'
    c = 'ec2-52-13-241-228.us-west-2.compute.amazonaws.com'
    docs = KafkaConsumer('docs', bootstrap_servers=c)

    # Create Elasticsearch instance
    es = Elasticsearch()

    for msg in docs:
        in_time = time.time()
        data = json.loads(msg.value.decode("utf-8").strip())

        doc = {
            "query" : {
                "percolate" : {
                    "field" : "query",
                    "document" : {
                        "message" : data['revision']
                    }
                }
            }
        }

        res = es.search(index="my-index", doc_type="_doc", body=doc)

        # Write matches to RethinkDB
        if res['hits']['total'] > 0:
            match = json.loads(msg.value.decode('utf-8').strip())
            #print(match)
            for hit in res['hits']['hits']:
                print("Found one!")
                found_match = r.table("queries").insert([{
                    "query_id": hit['_id'],
                    "article": match['article'],
                    "user_id": match['user_id'],
                    "found_time": int((time.time() - in_time)*1000)
                }]).run(conn)
                #print("time since entry:", int(time.time()*1000) - msg.timestamp)

        else:
            pass
            #print("Not match")



if __name__ == "__main__":
    search()





# query tweets
"""
import sys
import json
import time
import os
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import rethinkdb as r
import hashlib


if __name__ == "__main__":

    # db connection
    # conn = r.connect(host='ec2-54-202-215-9.us-west-2.compute.amazonaws.com', port='28015', db='alluvium')
    conn = r.connect(host='10.0.0.11', port='28015', db='alluvium')

    # Listen to Kafka docs topic
    #c = '10.0.0.14:9092'
    c = 'ec2-52-13-241-228.us-west-2.compute.amazonaws.com'
    docs = KafkaConsumer('tweets', bootstrap_servers=c)

    # Create Elasticsearch instance
    es = Elasticsearch()

    for msg in docs:
        in_time = time.time()
        data = json.loads(msg.value.decode("utf-8").strip())
        print(data)

        doc = {
            "query" : {
                "percolate" : {
                    "field" : "query",
                    "document" : {
                        # "message" : data['revision']
                        "message" : data['text']
                    }
                }
            }
        }

        res = es.search(index="my-index", doc_type="_doc", body=doc)

        print(res)

        # Write matches to RethinkDB
        if res['hits']['total'] > 0:
            match = json.loads(msg.value.decode('utf-8').strip(','))
            #print(match)
            for hit in res['hits']['hits']:
                print("Found one!")
                found_match = r.table("queries").insert([{
                    "query_id": hit['_id'],
                    "screen_name": match['screen_name'],
                    "followers": match['followers'],
                    "tweet_id": match['tweet_id'],
                    "compute_time": int((time.time() - in_time)*1000)
                }]).run(conn)
                print(found_match)

        else:
            pass
            #print("Not match")
"""


"""
from streamparse.bolt import Bolt
from elasticsearch import Elasticsearch
import sys
import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from streamparse.bolt import Bolt

class QuerySearchBolt(Bolt):

    #conn = 'ec2-52-13-241-228.us-west-2.compute.amazonaws.com:9092'
    conn = '10.0.0.14:9092'
    docs = KafkaConsumer('docs', bootstrap_servers=conn)
    producer = KafkaConsumer('match', bootstrap_servers=conn)
    es = Elasticsearch()

    def process(self, tup):
        data = json.loads(tup.value.decode("utf-8").strip(','))

        doc = {
            "query" : {
                "percolate" : {
                    "field" : "query",
                    "document" : {
                        "message" : "{}".format(data['revision'])
                    }
                }
            }
        }

        res = es.search(index="my-index", doc_type="_doc", body=doc)
        #If match - write do RethinkDB
        print(res)
"""
