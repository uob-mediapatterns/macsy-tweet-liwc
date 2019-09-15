from __future__ import division

import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC
from macsy_tweet_liwc.core import get_docs, liwc_docs, better_generator
import dateutil.parser
import json
from bson.objectid import ObjectId
import logging
import io
import numpy as np
import csv
import sys
import pickle

import hashlib

# Yes I know my module is called macsy-tweet-liwc...
# But it works on articles now, should just be macsy-liwc

# throws out docs with no text or ones with text we have already seen
@better_generator
def only_good(docs):
    s = set()
    yield
    while True:
        doc = docs.send(None)
        if doc.get('C', None) is None:
            continue

        h = hashlib.md5(doc['C'].encode("utf-8")).hexdigest()
	if h in s:
            continue

        s.add(h)

        yield doc

@better_generator
def extract():
    doc = yield
    while True:
        _id  = doc['_id']
        text = doc['C']
        doc = yield (text, _id)

@better_generator
def unnumpy():
    _id, vector, total, total_dic = yield
    while True:
        _id, vector, total, total_dic = yield (_id, vector.tolist(), total, total_dic)

def pipeline(liwc, bbapi, db, filter):
    # use the or operator here?
    docs = ( only_good(get_docs(bbapi, filter, "ARTICLE"))
           * extract()
           * liwc_docs(liwc, normalize=False, compute_values=False))

    return docs

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

def worker(macsy_settings, liwc_dict):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    filter = {
        "Fds": 1192 # NYT Opinion feed
    }

    p = pipeline(liwc, bbapi, db, filter)

    pickle.dump(list(p), open("/data/oped.pkl", "wb"), protocol=pickle.HIGHEST_PROTOCOL)

# Make sure to use server11 mongo settings (47017), and we only have data for 2019
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)

    args = parser.parse_args()

    worker(args.macsy_settings, args.liwc_dict)
