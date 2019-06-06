from __future__ import division

from macsy.api import BlackboardAPI

from liwc import LIWC, tokenize

import pymongo.errors
import json

from bson.son import SON

import numpy as np

from datetime import datetime, timedelta

import logging

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

def better_generator(f):
    def f_(*args, **kwargs):
        g = f(*args, **kwargs)
        g.send(None)
        return BetterGenerator(g)
    return f_

# why not use this as decorator directly?
# it has to have two different call implementations, one to do the function call, then one to do the
# generator call. in a unified system all generators would have trivial first call
# maybe they get passed a self sentinel which gets swapped out by the class?
# TODO Add from generator which takes existing generator not in our form, and makes it into the form?
class BetterGenerator:
    def __init__(self, g):
        self.g = g

    def __iter__(self):
        return self

    def __next__(self):
        return self.send(None)

    def __call__(self, v):
        return self.send(v)

    def send(self, v):
        return self.g.send(v)

    @better_generator
    def __mul__(self, other):
        i = yield
        while True:
            a_o = self.send(i)
            b_o = other.send(a_o)
            i = yield b_o

# I should move this into macsy as a 'robust' search, because it can recover from failures
@better_generator
def get_tweets(bbapi, filter={}):
    yield
    tweets_col = bbapi.load_blackboard("TWEET").document_manager.get_collection()

    # last_id is the last ID this produced, not just recieved
    last_id = None
    while True:
        if last_id is None:
            filter_ = filter
        else:
            filter_ = {
                "$and": [
                    {"_id": {"$gt" : last_id}},
                    filter
                ]
            }

        try:
            tweets = tweets_col.find(filter_).sort([('_id', 1)])

            for tweet in tweets:
                yield tweet
                last_id = tweet['_id']
        except pymongo.errors.OperationFailure as e:
            logging.exception(e)
            logging.debug("Retrying from {}".format(last_id))

@better_generator
def filter_and_tag(tweets):
    yield
    while True:
        tweet = tweets.send(None)
        _id  = tweet['_id']
        text = tweet['I']
        gender = tweet.get('G', 'n')
        loc    = tweet.get('L')
        if loc is None:
            continue

        loc = int(loc)

        yield (text, _id, gender, loc)

@better_generator
def liwc_tweets(liwc):
    tweet = yield
    while True:
        it = liwc.start()
        it.send(tokenize(tweet[0]))

        (vector, total, total_dic) = it
        values = liwc.human_values(vector)
        tweet = yield tweet[1:] + (vector, total, total_dic, values)


# potentially just make an accumulator do the grouping too?
# and then we just output time + tag + accumulated values

@better_generator
def group(liwc, bbapi, tweets):
    locs_col = bbapi.load_blackboard('LOCATION').document_manager.get_collection()
    locs = [int(v) for v in locs_col.distinct('_id')]
    
    mk_zero = lambda: (np.array([0 for _ in liwc.categories]), 0, 0, np.array([0 for _ in liwc.value_names]), 0)

    add = lambda a, b: tuple(va + vb for va,vb in zip(a,b))
    
    grouped = SON((str(loc), SON((gen, mk_zero()) for gen in "mfn")) for loc in locs)

    dt = None

    yield

    # only problem with this is it wont output the complete last until we get a new dt, tbh this isnt too bad, stops incomplete ones from being saved
    # maybe I should catch stopIteration when doing send on the below, which then forces save and quit?
    while True:
        # _id, gender, loc, rest
        tweet = tweets.send(None)
        _id, gender, loc = tweet[:3]
        loc = str(loc)

        tweet_dt = _id.generation_time.replace(minute=0, second=0, microsecond=0)

        if dt is None:
            dt = tweet_dt

        while tweet_dt > dt:
            yield (dt, grouped)
            grouped = SON((str(loc), SON((gen, mk_zero()) for gen in "mfn")) for loc in locs)
            dt += timedelta(hours=1)

        grouped[loc][gender] = add(grouped[loc][gender], tweet[3:] + (1,))

@better_generator
def saver(db):
    def remove_np(i):
        return tuple(list(v.astype(float)) if isinstance(v, np.ndarray) else v for v in i)

    def clean(grouped):
        return SON((k, SON((kk,remove_np(vv)) for kk, vv in v.items())) for k, v in grouped.items())

    (dt, grouped) = yield

    grouped = clean(grouped)

    while True:
        doc = SON((
            ("_id", SON((
                ("series_id", "liwc_tweet"),
                ("x", dt)
            ))),
            ("y", grouped),
        ))

        try:
            db["SWDATASERIES"].insert(doc)
        except pymongo.errors.OperationFailure as e:
            logging.exception(e)
            logging.debug("Retrying")
            continue

        # maybe output summary info
        # sum total tweets for the period?
        (dt, grouped) = yield
        grouped = clean(grouped)

def combined():
    with open('LIWC2007.txt', 'r') as liwc_file:
        liwc = LIWC(liwc_file)

    bbapi, db = load('settings.json')

    tweets             = get_tweets(bbapi)
    filtered           = filter_and_tag(tweets)
    filtered_with_liwc = filtered * liwc_tweets(liwc)
    grouped            = group(liwc, bbapi, filtered_with_liwc)
    g                  = grouped * saver(db)

    return g