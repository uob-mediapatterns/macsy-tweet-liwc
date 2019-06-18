from __future__ import division

from liwc import tokenize

import pymongo.errors

from bson.son import SON

import numpy as np

from datetime import datetime, timedelta

import logging

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

    def next(self):
        return self.__next__()

    def __call__(self, v):
        return self.send(v)

    def send(self, v):
        return self.g.send(v)

    # Really this assumes they're both infinite
    @better_generator
    def __mul__(self, other):
        i = yield
        while True:
            a_o = self.send(i)
            b_o = other.send(a_o)
            i = yield b_o

    # Assumes the second is infinite
    @better_generator
    def __add__(self, other):
        i = yield
        while True:
            try:
                i = yield self.send(i)
            except StopIteration:
                break
        while True:
            i = yield other.send(i)

    def __or__(self, other):
        other.send(self)
        return other

# I should move this into macsy as a 'robust' search, because it can recover from failures
@better_generator
def get_tweets(bbapi, filter={}):
    yield
    tweets_col = bbapi.load_blackboard("TWEET").document_manager.get_collection()

    last_id = None
    count = 0
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
                count += 1
                if count % 5000 == 0:
                    logging.debug("{}: {}".format(count, tweet['_id'].generation_time))


            break
        except pymongo.errors.OperationFailure as e:
            logging.exception(e)
            logging.debug("Retrying from {}".format(last_id))

# TODO Just like with gender, add an unknown location value?
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
def liwc_tweets(liwc, normalize=True, compute_values=True):
    tweet = yield
    while True:
        it = liwc.start(normalize=normalize)
        it.send(tokenize(tweet[0]))

        (vector, total, total_dic) = it
        if compute_values:
            values = liwc.human_values(vector)
            result = tweet[1:] + (vector, total, total_dic, values)
        else:
            result = tweet[1:] + (vector, total, total_dic)

        tweet = yield result

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
            db["SWDATASERIES"].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        except pymongo.errors.OperationFailure as e:
            logging.exception(e)
            logging.debug("Retrying")
            continue

        total_tweets = sum(gen[-1] for loc in doc["y"].values() for gen in loc.values())

        (dt, grouped) = yield (doc["_id"], total_tweets)
        grouped = clean(grouped)

def pipeline(liwc, bbapi, db, filter):
    tweets             = get_tweets(bbapi, filter)
    filtered           = filter_and_tag(tweets)
    filtered_with_liwc = filtered * liwc_tweets(liwc)
    grouped            = group(liwc, bbapi, filtered_with_liwc)
    g                  = grouped * saver(db)

    return g
