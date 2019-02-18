import redis


REDIS_SERVER = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"

def convertUnixtimestamp(timestamp):
    try:
        timestamp = int(timestamp)
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return str(timestamp)

def getLatestNews():
    rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
    ids = rdb.zrevrangebyscore("newsIdOrderedByTimestamp", "+inf", 980000000, withscores=False)
    output = []
    for id in ids[:10]:
        temp = {}
        news = rdb.hgetall("news:{}".format(id))
        if news is None:
            continue
        try:
            temp['headline'] = news['headline']
        except Exception as e:
            continue
        try:
            temp['body'] = news['body']
        except Exception as e:
            pass
        try:
            temp['tag_company'] = news['tag_company']
        except Exception as e:
            pass
        try:
            temp['timestamp'] = convertUnixtimestamp(news['timestamp'])
        except Exception as e:
            pass

        temp['numDups'] = rdb.hlen("dup_cand:{}".format(id))
        if temp['numDups'] > 0:
            temp['dupCands'] = rdb.hgetall("dup_cand:{}".format(id))
        else:
            temp['dupCands'] = {}

        output.append(temp)
    return output


def main():
    output = getLatestNews()
    print(output)

if __name__ == '__main__':
    main()
