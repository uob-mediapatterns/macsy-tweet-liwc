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

    # ObjectIDs would benefit from a different chunk size
    # Would make it way more complicated however
    chunk_size = 1024

    # I know unnormalized indicators and wcs are only ever integers...
    # But they're already stored as floats
    indicators = f.create_dataset("indicators", (0, len(labels)), maxshape=(None, len(labels)), chunks=(chunk_size,len(labels)), dtype=np.float64, compression="gzip", compression_opts=9)
    wordcounts = f.create_dataset("wordcounts", (0,), maxshape=(None,), chunks=(chunk_size,), dtype=np.float64)
    objectids  = f.create_dataset("objectids", (0, 12), maxshape=(None, 12), chunks=(chunk_size,12), dtype=np.uint8)

    indicators_chunk = np.zeros((chunk_size, len(labels)), dtype=np.float64)
    objectids_chunk = np.zeros((chunk_size, 12), dtype=np.uint8)
    wordcounts_chunk = np.zeros((chunk_size,), dtype=np.float64)

    p = iter(pipeline(liwc, bbapi, db, filter))
    done = False
    while not done:
        start = indicators.shape[0]
        inserted = 0
        for i in range(chunk_size):
            try:
                _id, _, vector, wordcount, _ = next(p)
            except StopIteration:
                done = True
                break

            # No longer storing word count
            indicators_chunk[i,:] = vector
            objectids_chunk[i,:] = np.frombuffer(_id.binary, dtype=np.uint8)
            wordcounts_chunk[i] = wordcount
            inserted += 1

        indicators.resize(start + inserted, 0)
        objectids.resize(start + inserted, 0)
        wordcounts.resize(start + inserted, 0)

        indicators[start:start+inserted] = indicators_chunk[0:inserted]
        objectids[start:start+inserted] = objectids_chunk[0:inserted]
        wordcounts[start:start+inserted] = wordcounts_chunk[0:inserted]

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
