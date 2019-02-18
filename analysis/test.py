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
    output = {}
    for id in ids[:10]:
        output[id] = {}
        news = rdb.hgetall("news:{}".format(id))
        if news is None:
            continue
        try:
            output['headline'] = news['headline']
        except Exception as e:
            continue
        try:
            output['body'] = news['body']
        except Exception as e:
            pass
        try:
            output['tag_company'] = news['tag_company']
        except Exception as e:
            pass
        try:
            output['timestamp'] = convertUnixtimestamp(news['timestamp'])
        except Exception as e:
            pass

        output['numDups'] = rdb.hlen("dup_cand:{}".format(id))
        if output['numDups'] > 0:
            output['dupCands'] = rdb.hgetall("dup_cand:{}".format(id))
        else:
            output['dupCands'] = {}

    return output


def main():
    output = getLatestNews()
    print(output)

if __name__ == '__main__':
    main()
