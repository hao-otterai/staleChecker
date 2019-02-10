from app import app
from flask import render_template, redirect
import datetime
import redis
import math
from collections import Counter

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "/src/config")
import config

rdb = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=0)



''' Utility functions '''
def calc_likelihood(sim_score):
    likelihoods = [("Low", "btn-default"), ("Medium", "btn-warning"), ("High", "btn-danger")]
    print(sim_score)
    partition = config.DUP_QUESTION_IDENTIFY_THRESHOLD / (len(likelihoods) - 1)
    print(partition)
    print(int(math.floor(sim_score // partition)))
    return likelihoods[min(len(likelihoods) - 1, int(math.floor(sim_score // partition)))]


def so_link(qid):
    return "http://stackoverflow.com/q/{0}".format(qid)


def format_dup_cand(dc):
    dc_info = eval(dc[0])
    dc_sim = dc[1]
    llh_rating, llh_button = calc_likelihood(dc_sim)
    return {
        # "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p"),
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

# for test purpose only
@app.route("/hello")
def hello():
    return "Hello from flask!"

@app.route('/countme/<input_str>')
def count_me(input_str):
    input_counter = Counter(input_str)
    response = []
    for letter, count in input_counter.most_common():
        response.append('"{}": {}'.format(letter, count))
    return '<br>'.join(response)

@app.route('/github')
def github():
    return redirect("https://github.com/haoyang09/staleChecker.git")


# @app.route("/")
# @app.route("/candidates")
# def candidates():
#     all_cands = rdb.zrevrangebyscore("dup_cand", "+inf", config.DUP_QUESTION_SHOW_THRESHOLD, withscores=True)
#     dup_cands = [format_dup_cand(dc) for dc in all_cands]
#     return render_template("q_list.html", dup_cands=dup_cands)
#
#
# # @app.route("/about")
# # def about():
# #     return render_template("about.html")
# #
# #
# # @app.route("/visualization")
# # def visualization():
# #     return render_template("q_cluster_visualization.html")
# #
# #
# # @app.route("/metrics")
# # def metrics():
# #     return render_template("metrics.html")
#
#
# @app.route('/slides')
# def slides():
#     return redirect("https://bit.ly/2I5yGPT")
