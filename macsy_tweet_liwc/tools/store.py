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

@better_generator
def extract():
    tweet = yield
    while True:
        _id  = tweet['_id']
        text = tweet['I']
        location = tweet.get('L', 'n')

        tweet = yield (text, _id, location)

def pipeline(liwc, bbapi, db, filter):
    tweets = ( get_tweets(bbapi, filter)
             * extract()
             * liwc_tweets(liwc, normalize=True, compute_values=False))

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

    # We only want tweets for the UK-wide pipeline
    filter = {
        "_id": _id_filter,
        "L": {
            "$in": list(range(1,55))
        }
    }



    tweets_col = bbapi.load_blackboard("TWEET").document_manager.get_collection()
    
    f = h5py.File("uk_pipeline_tweet_liwc_2_year.hdf5", "w")

    labels = f.create_dataset("labels", (len(liwc.categories),), dtype=h5py.string_dtype())
    labels[:] = [v for v,_ in liwc.categories.values()]

    indicators = f.create_dataset("indicators", (0, len(labels)), maxshape=(None, len(labels)), dtype=np.float64)
    objectids  = f.create_dataset("objectids", (0, 12), maxshape=(None, 12), dtype=np.uint8)

    p = pipeline(liwc, bbapi, db, filter)
    for i, o in enumerate(p):
        _id, _, vector, _, _ = o
        
        indicators.resize(i + 1, 0)
        objectids.resize(i + 1, 0)

        # No longer storing word count
        indicators[i,:] = vector
        objectids[i,:] = np.frombuffer(_id.binary, dtype=np.uint8)

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
