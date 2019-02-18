from app import app
from flask import render_template, redirect
from collections import Counter

from datetime import datetime
import redis
import math

import sys
import os

REDIS_SERVER = "ec2-54-189-255-59.us-west-2.compute.amazonaws.com"

''' Basic Routes '''
@app.route('/test')
def index():
    return "Hello from flask!"

@app.route('/countme/<input_str>')
def count_me(input_str):
    input_counter = Counter(input_str)
    response = []
    for letter, count in input_counter.most_common():
        response.append('"{}": {}'.format(letter, count))
    return '<br>'.join(response)

@app.route('/slides')
def slides():
    return redirect("https://bit.ly/2WOw78n")
#
@app.route('/github')
def github():
    return redirect("https://github.com/haoyang09/staleChecker.git")

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/metrics")
def metrics():
    return render_template("metrics.html")


''' util functions '''
def convertUnixtimestamp(timestamp):
    try:
        timestamp = int(timestamp)
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return str(timestamp)


def getNewsDetails(news_id):
    rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
    res = {}
    res['id'] = news_id

    news = rdb.hgetall("news:{}".format(news_id))
    try:
        res['headline'] = news['headline']
    except Exception as e:
        res['headline'] = ''
    try:
        res['body'] = news['body']
    except Exception as e:
        res['body'] = ''
    try:
        res['tag_company'] = news['tag_company'].split(',')
    except Exception as e:
        res['tag_company'] = []
    try:
        res['timestamp'] = convertUnixtimestamp(news['timestamp'])
    except Exception as e:
        res['timestamp'] = ''
    res['numDups'] = rdb.hlen("dup_cand:{}".format(news_id))

    if res['numDups'] > 0:
        res['dupCands'] = rdb.hgetall("dup_cand:{}".format(news_id))
    else:
        res['dupCands'] = []

    return res


''' Routes '''
@app.route('/')
@app.route('/latest')
def latestNews():
    rdb = redis.StrictRedis(REDIS_SERVER, port=6379, db=0)
    ids = rdb.zrevrangebyscore("newsId", "+inf", 980380000, withscores=False)
    output = []
    for id in ids[:250]:
        output.append(getNewsDetails(id))
    return render_template("news_list.html", dup_cands=output)


@app.route('/news/<news_id>')
def singleNewsView(news_id):
    news_id = str(news_id)
    news = getNewsDetails(news_id)
    news['dupCandDetails'] = []
    for id in news['dupCands']:
        news['dupCandDetails'].append(getNewsDetails(id))

    return render_template("news_detail.html", news=news)


@app.route('/tag/<tag>')
def singleTagView(tag):
    ids = rdb.smembers("lsh:{0}".format(tag))
    output = []
    for id ids:
        output.append(getNewsDetails(id))
    return render_template("news_detail.html", dup_cands=output)
