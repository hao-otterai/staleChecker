import redis
import collections
import operator
import pickle
import os
import matplotlib.pyplot as plotter
from tqdm import tqdm

REDIS_SERVER = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"


# def sortNewsIdRedis():
#     rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
#     # rdb.sadd("newsId", news.id)
#     # rdb.hmset("news:{}".format(news.id), save_content)
#     ids = rdb.smembers("newsId")
#     print('number of news ids in newsID: {}'.format(rdb.scard("newsId")))
#     for id in tqdm(ids):
#         timestamp = rdb.hget("news:{}".format(id), 'timestamp')
#         if timestamp is not None:
#             rdb.zadd("newsIdOrderedByTimestamp", int(timestamp), id)
#     print('number of news ids in newsIdOrderedByTimestamp: {}'.format(rdb.zcard("newsIdOrderedByTimestamp")))


# def data_stats():
#     rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
#     idsAll = rdb.smembers('newsId')
#     idsTag = rdb.smembers("lsh:{}".format(tag))

def tagCounter():
    rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
    c = collections.Counter()
    for lsh_key in rdb.sscan_iter("lsh_keys", match="*", count=500):
        tag = lsh_key.decode('utf-8').replace("lsh:", "")
        c[tag] = rdb.scard("lsh:{0}".format(tag))
    return dict(sorted(c.items(), key=operator.itemgetter(1), reverse=True))


def drawPieChart(data, labels):
    figureObject, axesObject = plotter.subplots()
    # Draw the pie chart
    axesObject.pie(data, labels=labels, autopct='%1.2f', startangle=90)
    # Aspect ratio - equal means pie is a circle
    axesObject.axis('equal')
    plotter.show()


def main():
    # get tags distributions
    tagsCountFile = '/home/hao/staleChecker/analysis/tagsCount.pickle'
    if os.path.exists(tagsCountFile):
        with open(tagsCountFile, 'rb') as handle:
            tags = pickle.load(handle)
    else:
        tags = tagCounter()
        with open(tagsCountFile, 'wb') as handle:
            pickle.dump(tags, handle, protocol=pickle.HIGHEST_PROTOCOL)

    drawPieChart(list(tags.values())[:10],  list(tags.keys())[:10])
    #drawPieChart(list(tags.values())[1:16],  list(tags.keys())[1:16])

    print(list(zip(list(tags.values())[:10],  list(tags.keys())[:10])))

if __name__ == '__main__':
    main()
