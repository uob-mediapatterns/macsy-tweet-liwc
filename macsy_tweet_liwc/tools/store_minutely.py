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

import h5py
import ast

@better_generator
def extract():
    tweet = yield
    while True:
        _id  = tweet['_id']
        text = tweet['I']

        tweet = yield (text, _id)

def pipeline(liwc, bbapi, db, filter, blackboard):
    tweets = ( get_tweets(bbapi, filter, blackboard=blackboard)
             * extract()
             * liwc_tweets(liwc, normalize=True, compute_values=False))

    return tweets

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

def find_bucket(t):
    return t.replace(second=0, microsecond=0)

def worker(liwc_dict, start_date, end_date, f):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    macsy_settings = f["tweets"].attrs["macsy_settings"]
    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    _id_filter = {
        "$gte": ObjectId.from_datetime(start_date),
        "$lt": ObjectId.from_datetime(end_date)
    }
    
    filter = ast.literal_eval(f["tweets"].attrs["filter"])
    filter["_id"] = _id_filter

    blackboard = f["tweets"].attrs["blackboard"]
    tweets_col = bbapi.load_blackboard(blackboard).document_manager.get_collection()

    f["tweets"].attrs["inserted"] = np.asarray(f["tweets"].attrs["inserted"].tolist() + ["{} to {}".format(start_date.isoformat(), end_date.isoformat())], dtype=h5py.string_dtype())

    # times should contain UTC isoformated times
    times = f["tweets"]["times"]
    buckets_lookup = dict((dt, i) for i, dt in enumerate(times))
    
    indicators  = f["tweets"]["indicators"]
    wordcounts  = f["tweets"]["wordcounts"]
    tweetcounts = f["tweets"]["tweetcounts"]

    # Make sure indicators are normalized
    p = pipeline(liwc, bbapi, db, filter, blackboard)

    for _id, vector, wordcount, _ in p:
        i = buckets_lookup.get(find_bucket(_id.generation_time).isoformat(), None)
        if i is None:
            continue

        indicators[i,:] += vector
        wordcounts[i]   += wordcount
        tweetcounts[i]  += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("hdf5", help="hdf5 file", type=str)
    parser.add_argument("start_date", help="start date", type=str)
    parser.add_argument("end_date", help="end date", type=str)

    args = parser.parse_args()

    start_date = dateutil.parser.parse(args.start_date)
    end_date = dateutil.parser.parse(args.end_date)

    with h5py.File(args.hdf5, "r+") as f:
        worker(args.liwc_dict, start_date, end_date, f)
