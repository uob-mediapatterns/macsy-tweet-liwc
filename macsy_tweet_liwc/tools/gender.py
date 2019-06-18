from __future__ import division

import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC
from macsy_tweet_liwc.core import get_tweets, liwc_tweets, better_generator
import dateutil.parser
import json
from bson.objectid import ObjectId
import logging
import io
import numpy as np
import csv
import sys

@better_generator
def welford(zero):
    # Ensure floating point, for both arrays and scalars
    zero = zero.astype(np.float64)

    k = 0
    M = zero
    S = zero

    def get():
        if k < 2:
            return (np.nan, np.nan, np.nan)

        mean = M
        std  = np.sqrt(S/(k-1))
        return (mean, std, k)
    
    action = yield

    while True:
        if action is None:
            response = get()
        else:
            x = action
            k += 1
            newM = M + (x - M)/k
            newS = S + (x - M)*(x - newM)
            M, S = newM, newS
            response = None

        action = yield response

'''
TODO Remove passing zero, make it somethign you put into the accumulator before starting
'''
@better_generator
def mean_and_std(zero):
    k = 0

    data = None

    def get():
        if k < 2:
            return (np.nan, np.nan, np.nan)

        # TODO Use numpy mean and std to get it
        return (mean, std, k)
    
    action = yield

    while True:
        if action is None:
            response = get()
        else:
            x = action
            if k == 0:
                data = x
            else:
                data = np.vstack((data, x))
            response = None

        action = yield response

@better_generator
def extract():
    tweet = yield
    while True:
        _id  = tweet['_id']
        text = tweet['I']
        gender = tweet.get('G', 'n')

        tweet = yield (text, _id, gender)

#@better_generator
#def drop_unknown_gender():
#    tweets = yield
#    while True:
#        tweet = tweets.send(None)
#        if tweet[2] in "mf":
#            yield tweet

'''
TODO

Add alternative accumulate which stores all values and then sums at the end
I estimated 35GB of ram required, which is fine on the servers

Probably best to make it a flag via argparse

'''
@better_generator
def accumulate(liwc, accumulator=welford):
    # LIWC, Schwartz, wc, wc_dic
    genders = {g: accumulator(np.zeros(len(liwc.categories) + len(liwc.value_names) + 2)) for g in "mfn"}

    def get():
        return {g: v.send(None) for g, v in genders.items()}

    def update(action):
        _id, gender, vector, total, total_dic, values = action
        return genders[gender].send(np.concatenate((vector, values, [total, total_dic])))

    action = yield

    while True:
        if action is None:
            response = get()
        else:
            response = update(action)

        action = yield response

@better_generator
def once(v):
    yield
    yield v

def pipeline(liwc, bbapi, db, filter):
    tweets = ( get_tweets(bbapi, filter)
             * extract()
             * liwc_tweets(liwc))

    g = (tweets + once(None)) * accumulate(liwc)
    return g

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

def worker(macsy_settings, liwc_dict, start_date, end_date):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    # Apparently mongo doesnt care about key order in AND, so compound index
    # should still apply
    filter = {
        "_id": {
            "$gte": ObjectId.from_datetime(start_date),
            "$lt": ObjectId.from_datetime(end_date)
        },
        "G": {
            "$in": ["f","m"]
        }
    }

    p = pipeline(liwc, bbapi, db, filter)

    labels = [v for v,_ in liwc.categories.values()] + liwc.value_names + ["wc","wc_dic"]
    labels = ["gender"] + ["{}_{}".format(l,t) for t in ["mean","std"] for l in labels] + ["num_tweets"]

    with open("result.csv","w") as f:
        writer = csv.writer(f)
        writer.writerow(labels)
        for o in p:
            if o is None:
                continue

            writer.writerow(['m'] + list(o['m'][0]) + list(o['m'][1]) + [o['m'][2]])
            writer.writerow(['f'] + list(o['f'][0]) + list(o['f'][1]) + [o['f'][2]])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("start_date", help="start date", type=str)
    parser.add_argument("end_date", help="end date", type=str)

    args = parser.parse_args()

    start_date = dateutil.parser.parse(args.start_date)
    end_date = dateutil.parser.parse(args.end_date)

    worker(args.macsy_settings, args.liwc_dict, start_date, end_date)
