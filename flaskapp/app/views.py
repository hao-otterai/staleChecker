from app import app
from flask import render_template, redirect
from datetime import datetime
import redis
import math
from collections import Counter

import sys
import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "/src/config")
# import config

REDIS_SERVER = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"
DUP_QUESTION_IDENTIFY_THRESHOLD = 0.6
DUP_QUESTION_SHOW_THRESHOLD = 0.65


''' Utility functions '''
def calc_likelihood(sim_score):
    likelihoods = [("Low", "btn-default"), ("Medium", "btn-warning"), ("High", "btn-danger")]
    print(sim_score)
    partition = DUP_QUESTION_IDENTIFY_THRESHOLD / (len(likelihoods) - 1)
    print(partition)
    print(int(math.floor(sim_score // partition)))
    return likelihoods[min(len(likelihoods) - 1, int(math.floor(sim_score // partition)))]


def sortNewsIdRedis():
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    # rdb.sadd("newsId", news.id)
    # rdb.hmset("news:{}".format(news.id), save_content)
    ids = rdb.smembers("newsId")
    print('number of news ids in newsID: {}'.format(rdb.scard("newsId")))
    for id in ids:
        timestamp = rdb.hget("news:{}".format(id), 'timestamp')
        if timestamp is not None:
            rdb.zadd("newsIdOrderedByTimestamp", timestamp, id)
    print('number of news ids in newsIdOrderedByTimestamp: {}'.format(rdb.zcard("newsIdOrderedByTimestamp")))


def convertUnixtimestamp(timestamp):
    try:
        timestamp = int(timestamp)
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return str(timestamp)

# def so_link(qid):
#     return "http://stackoverflow.com/q/{0}".format(qid)

def format_dup_cand(dc):
    dc_info = eval(dc[0])
    dc_sim = dc[1]
    llh_rating, llh_button = calc_likelihood(dc_sim)
    return {
        # "timestamp": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
        "tag": dc_info[0].capitalize(),
        "q1_id": dc_info[1],
        "q1_title": dc_info[2],
        "q2_id": dc_info[3],
        "q2_title": dc_info[4],
        "q1_link": so_link(dc_info[1]),
        "q2_link": so_link(dc_info[3]),
        "likelihood_button": llh_button,
        "likelihood_rating": llh_rating,
        "timestamp": dc_info[5]
    }


''' Routes '''
@app.route('/')
@app.route('/index')
def index():
    return "Hello from flask!"


@app.route('/countme/<input_str>')
def count_me(input_str):
    input_counter = Counter(input_str)
    response = []
    for letter, count in input_counter.most_common():
        response.append('"{}": {}'.format(letter, count))
    return '<br>'.join(response)


@app.route("/allnews")
def allNews():
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    ids = rdb.zrevrangebyscore("newsIdOrderedByTimestamp", "+inf", 980000000, withscores=True)
    #dup_cands = [rdb.zrevrangebyscore("dup_cand:{}".format(id[0]), 1.0, DUP_QUESTION_SHOW_THRESHOLD, withscores=True) for id in ids]
    #dup_cands = [format_dup_cand(dc) for id in ids]
    return render_template("q_list.html", dup_cands=dup_cands)


@app.route('/dup/<news_id>')
def getDupCands(news_id):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    dups = "dup_cand:{}".format(id)


@app.route('/news/<news_id>')
def newsView(news_id):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    news = redis.hgetall("news:{}".format(news_id))


#@app.route("/")
@app.route("/candidates")
def candidates():
    all_cands = rdb.zrevrangebyscore("dup_cand", "+inf", DUP_QUESTION_SHOW_THRESHOLD, withscores=True)
    dup_cands = [format_dup_cand(dc) for dc in all_cands]
    return render_template("q_list.html", dup_cands=dup_cands)

@app.route('/slides')
def slides():
    return redirect("https://bit.ly/2WOw78n")
#
@app.route('/github')
def github():
    return redirect("https://github.com/haoyang09/staleChecker.git")




# @app.route("/about")
# def about():
#     return render_template("about.html")
#
#
# @app.route("/visualization")
# def visualization():
#     return render_template("q_cluster_visualization.html")
#
#
# @app.route("/metrics")
# def metrics():
#     return render_template("metrics.html")
