from __future__ import division

import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC
from macsy_tweet_liwc.core import get_tweets, liwc_tweets, better_generator
import dateutil.parser
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta, MO
import json
from bson.objectid import ObjectId
import logging
import io
import numpy as np
import csv
import sys

'''
Could make it a non-datebased collection
Do I transpose xs? potentially easier

{
    _id: Int,
    interval: String,
    num_interval: Int, # eg, for a year it's 52, and interval would be "week"
    xs: [
        [ Int, Int, Int, Int ... ],
        [ Int, Int, Int, Int ... ],
        [ Int, Int, Int, Int ... ],
        ...
    ],
    ys: [ Date, Date, Date, Date... ], # len(ys) === num_interval
    state: {
        k: Int,
        M: Double,
        last_updated: Date, # really this is the timestamp for the last tweet we found?
    },
}



'''
def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

@better_generator
def extract():
    tweet = yield
    while True:
        _id  = tweet['_id']
        text = tweet['I']

        tweet = yield (text, _id)

def pipeline(liwc, bbapi, filter):
    tweets = ( get_tweets(bbapi, filter)
             * extract()
             * liwc_tweets(liwc))

    return tweets

def intervalToRelativeDelta(interval):
    if interval == "hour":
        return relativedelta(hours=1)
    elif interval == "day":
        return relativedelta(days=1)
    elif interval == "week":
        return relativedelta(weeks=1)

def floor_dt(interval, dt):
    if interval == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif interval == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == "week":
        return floor_day(dt) + relativedelta(weekday=MO(-1))


def worker(macsy_settings, liwc_dict):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    now = datetime.now(tz=timezone.utc)

    documents = list(db["TWEET_LIWC_LIVE"].find())

    # convert to numpy as it's easier to work with
    # make sure to convert back when saving to mongo
    # change datetime to UTC
    for doc in documents:
        doc["state"]["M"] = np.array(doc["state"]["M"], dtype=np.float64)
        doc["state"]["last_updated"] = doc["state"]["last_updated"].replace(tzinfo=timezone.utc)
        doc["num_interval"] = int(doc["num_interval"])

    # for each last updated, set it to the greater of last updated and oldest possible interval
    oldest = []
    for doc in documents:
        last_updated = doc["state"]["last_updated"]
        oldest_possible = floor_dt(doc["interval"], now) - doc["num_interval"]*intervalToRelativeDelta(doc["interval"])
        if last_updated < oldest_possible:
            oldest.append(oldest_possible)
        else:
            oldest.append(last_updated)

    
    # then, starting with the oldest last updated, go through tweets
    # only work with tweets extraction from the 52 locations
    filter = {
        "_id": {
            "$gte": ObjectId.from_datetime(min(oldest)),
        },
        "L": {
            "$exists": True,
        }
    }

    # NOTE Tweet isnt an object, it's a tuple
    # _id, vector, wc, wc_dic, values (last is only there if we ask for it)
    for (_id, vector, _, _, _) in pipeline(liwc, bbapi, filter):
        # for each (zipped) oldest + document
        for oldestv, doc in zip(oldest, documents):
            # if tweet is AFTER last updated
            if _id.generation_time > oldestv:
                start = floor_dt(doc["interval"], doc["state"]["last_updated"])
                end   = start + intervalToRelativeDelta(doc["interval"])
                while not (start <= _id.generation_time < end):
                    x = list(doc["state"]["M"]) # m is a vector, remember
                    y = start

                    doc["xs"] = doc["xs"][-doc["num_interval"]+1:] + [x]
                    doc["ys"] = doc["ys"][-doc["num_interval"]+1:] + [y]

                    doc["state"]["M"] = np.zeros(6, dtype=np.float64)
                    doc["state"]["k"] = 0
                    doc["state"]["last_updated"] = end

                    start = floor_dt(doc["interval"], doc["state"]["last_updated"])
                    end   = start + intervalToRelativeDelta(doc["interval"])

                # add tweet to state
                doc["state"]["k"] += 1
                doc["state"]["M"] += (vector[26:26+6] - doc["state"]["M"]) / doc["state"]["k"]
                doc["state"]["last_updated"] = _id.generation_time
    
    for doc in documents:
        # deal with case where there were no new tweets, but we need to insert zeros into the timeline
        # given the current time
        start = floor_dt(doc["interval"], doc["state"]["last_updated"])
        end   = start + intervalToRelativeDelta(doc["interval"])
        while not (start <= now < end):
            x = list(doc["state"]["M"]) # m is a vector, remember
            y = start

            doc["xs"] = doc["xs"][-doc["num_interval"]+1:] + [x]
            doc["ys"] = doc["ys"][-doc["num_interval"]+1:] + [y]

            doc["state"]["M"] = np.zeros(6, dtype=np.float64)
            doc["state"]["k"] = 0
            doc["state"]["last_updated"] = end

            start = floor_dt(doc["interval"], doc["state"]["last_updated"])
            end   = start + intervalToRelativeDelta(doc["interval"])

    for doc in documents:
        doc["state"]["M"] = list(doc["state"]["M"])

        db["TWEET_LIWC_LIVE"].find_one_and_replace({"_id": doc["_id"]}, doc)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)

    args = parser.parse_args()

    worker(args.macsy_settings, args.liwc_dict)
