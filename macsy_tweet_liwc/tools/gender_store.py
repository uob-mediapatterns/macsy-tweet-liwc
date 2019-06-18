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
def extract():
    tweet = yield
    while True:
        _id  = tweet['_id']
        text = tweet['I']
        gender = tweet.get('G', 'n')

        tweet = yield (text, _id, gender)

def pipeline(liwc, bbapi, db, filter):
    tweets = ( get_tweets(bbapi, filter)
             * extract()
             * liwc_tweets(liwc, normalize=False, compute_values=False))

    return tweets

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

    _id_filter = {
        "$gte": ObjectId.from_datetime(start_date),
        "$lt": ObjectId.from_datetime(end_date)
    }

    filter = {
        "_id": _id_filter,
        "G": {
            "$in": ["f","m"]
        }
    }

    p = pipeline(liwc, bbapi, db, filter)

    labels = [v for v,_ in liwc.categories.values()] + ["wc","wc_dic"]

    tweets_col = bbapi.load_blackboard("TWEET").document_manager.get_collection()
    
    # pls symlink
    genders = {}
    for g in"mf":
        count = tweets_col.find({"_id": _id_filter, "G": g}).count()
        genders[g] = {
            "fp": np.memmap("{}_{}x{}_tweets.npy".format(g, count, len(labels)), mode="w+", dtype=np.int64, shape=(count, len(labels))),
            "i": 0
        }

    for o in p:
        _id, gender, vector, total, total_dic = o
        fp = genders[gender]["fp"]
        i  = genders[gender]["i"]

        fp[i,:-2] = vector
        fp[i,-2:] = [total, total_dic]

        genders[gender]["i"] += 1

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
