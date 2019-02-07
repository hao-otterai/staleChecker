import redis

# step 2: define our connection information for Redis
# Replaces with your configuration information
redis_host = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"
#redis_host = "localhost"
redis_port = 6379
redis_password = ""


def hello_redis():
    """Example Hello Redis Program"""
    # step 3: create the Redis Connection object
    try:
        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        # step 4: Set the hello message in Redis
        r.set("msg:hello", "Hello Redis!!!")

        # step 5: Retrieve the hello message from Redis
        msg = r.get("msg:hello")
        print(msg)
    except Exception as e:
        print(e)


def basic_tests():
    print('Perform basic test of redis functions...')
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    import json
    for a,b,c in zip(range(1, 5), range(3,7), range(5,9)):
        json_d = json.dumps({"a":a, "b":b, "c":c})
        r.sadd('test_sadd', json_d)
        r.zadd('test_zadd', a+b+c, json_d)

    print(r.smembers('test_sadd'))
    print(r.zrangebyscore('test_zadd', '-inf', '+inf', withscores=False))

    print('test passed, redis connection is ok')

if __name__ == '__main__':
    hello_redis()
    basic_tests()
