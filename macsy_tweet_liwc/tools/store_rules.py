from __future__ import division

import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC, CountRules, tokenize
from macsy_tweet_liwc.core import get_tweets, liwc_tweets, better_generator
from dateutil.parser import parse as dtparse
from dateutil import rrule, tz
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

@better_generator
def count_rules_docs(countrules):
    doc = yield
    while True:
        it = countrules.start()
        it.send(tokenize(doc[0]))
        vector, total = it
        doc = yield doc[1:] + (vector, total)

def pipeline(countrules, bbapi, db, filter):
    tweets = ( get_tweets(bbapi, filter)
             * extract()
             * count_rules_docs(countrules))

    return tweets

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

minute_intervals = [0,15,30,45]
minute_intervals_reversed = list(reversed(minute_intervals))
def find_bucket(t):
    for i in minute_intervals_reversed:
        if t.minute >= i:
            return t.replace(minute=i, second=0, microsecond=0)

def worker(macsy_settings, liwc_dict, start_date, end_date):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    london = tz.gettz("Europe/London")

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    countrules = CountRules(liwc)

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

    earliest = next(iter(tweets_col.find(filter).sort([("_id", 1)]).limit(1)))["_id"].generation_time
    latest   = next(iter(tweets_col.find(filter).sort([("_id", -1)]).limit(1)))["_id"].generation_time

    earliest = find_bucket(earliest)
    latest   = find_bucket(latest)

    buckets = list(dt.replace(tzinfo=tz.UTC) for dt in rrule.rrule(rrule.MINUTELY, dtstart=earliest, until=latest) if dt.minute in minute_intervals)
    buckets_lookup = dict((dt.isoformat(), i) for i, dt in enumerate(buckets))

    f = h5py.File("uk_pipeline_tweet_liwc_2_year_rules.hdf5", "w")

    f.create_dataset("labels", data=np.asarray(countrules.rules, dtype=h5py.string_dtype()))
    f.create_dataset("times", data=np.asarray([t.astimezone(london).isoformat().replace('+00:00', 'Z') for t in buckets], dtype=h5py.string_dtype()))

    rules      = f.create_dataset("rules", (len(buckets), len(countrules.rules)), dtype=np.int64)
    wordcounts = f.create_dataset("wordcounts", (len(buckets),), dtype=np.int64)

    p = pipeline(countrules, bbapi, db, filter)

    for _id, _, vector, wordcount in p:
        i = buckets_lookup[find_bucket(_id.generation_time).isoformat()]
        rules[i,:] += vector
        wordcounts[i] += wordcount

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("start_date", help="start date", type=str)
    parser.add_argument("end_date", help="end date", type=str)

    args = parser.parse_args()

    start_date = dtparse(args.start_date)
    end_date = dtparse(args.end_date)

    worker(args.macsy_settings, args.liwc_dict, start_date, end_date)
