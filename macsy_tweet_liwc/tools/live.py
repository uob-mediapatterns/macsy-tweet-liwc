from __future__ import division

import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC
from macsy_tweet_liwc.core import get_tweets, liwc_tweets, better_generator
import dateutil.parser
from datetime import datetime
import pytz
from dateutil.relativedelta import relativedelta, MO
import json
from bson.objectid import ObjectId
import logging
import io
import numpy as np
import csv
import sys

'''
Could make it a non-datebased blackboard
Do I transpose xs? potentially easier

{
    _id: Int,
    interval: String,
    num_interval: Int, # eg, for a year it's 52, and interval would be "week"
    ys: [
        [ Float, Float, Float, Float ... ],
        [ Float, Float, Float, Float ... ],
        [ Float, Float, Float, Float ... ],
        ...
    ],
    xs: [ Date, Date, Date, Date... ], # len(xs) === len(ys) === num_interval
    next_x: Date, # used for display purposes
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
        return floor_dt("day", dt) + relativedelta(weekday=MO(-1))


def worker(macsy_settings, liwc_dict, blackboard):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    now = datetime.now(tz=pytz.utc)

    documents = list(db[blackboard].find())

    # convert to numpy as it's easier to work with
    # make sure to convert back when saving to mongo
    # change datetime to UTC
    for doc in documents:
        doc["state"]["M"] = np.array(doc["state"]["M"], dtype=np.float64)
        doc["state"]["last_updated"] = doc["state"]["last_updated"].replace(tzinfo=pytz.utc)
        doc["num_interval"] = int(doc["num_interval"])

    for doc in documents:
        last_updated = doc["state"]["last_updated"]
        oldest_possible = floor_dt(doc["interval"], now) - doc["num_interval"]*intervalToRelativeDelta(doc["interval"])
        if last_updated < oldest_possible:
            # If a document has become so old the state is no longer relavent, clear it
            doc["state"]["M"] = np.zeros(6, dtype=np.float64)
            doc["state"]["k"] = 0
            doc["state"]["xs"] = []
            doc["state"]["ys"] = []
            doc["state"]["last_updated"] = oldest_possible

    oldest = min(doc["state"]["last_updated"] for doc in documents)
    # Only do tweets up until 30 minutes ago, so we can be sure they have settled
    newest = now - relativedelta(minutes=30)
    
    # then, starting with the oldest last updated, go through tweets
    # only work with tweets extraction from the 52 locations
    filter = {
        "_id": {
            "$gte": ObjectId.from_datetime(oldest),
            "$lt": ObjectId.from_datetime(newest)
        },
        "L": {
            "$exists": True,
        }
    }

    # NOTE Tweet isnt an object, it's a tuple
    # _id, vector, wc, wc_dic, values (last is only there if we ask for it)
    for (_id, vector, _, _, _) in pipeline(liwc, bbapi, filter):
        for doc in documents:
            if _id.generation_time >= doc["state"]["last_updated"]:
                start = floor_dt(doc["interval"], doc["state"]["last_updated"])
                end   = start + intervalToRelativeDelta(doc["interval"])
                while _id.generation_time >= end:
                    x = start
                    y = list(doc["state"]["M"]) # m is a vector, remember

                    doc["xs"] = doc["xs"][-doc["num_interval"]+1:] + [x]
                    doc["next_x"] = end
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
    
    # Moved inserting empty xs/ys into the client side, as a graph shift

    for doc in documents:
        doc["state"]["M"] = list(doc["state"]["M"])

        db[blackboard].find_one_and_replace({"_id": doc["_id"]}, doc)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser("tweet-liwc-live", description="Pulls tweets from before 30 minutes ago from TWEET blackboard, processes them with LIWC, and takes period averages and places them into the target blackboard")
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("--blackboard", help="blackboard to store the series", nargs='?', const=1, default="TWEET_LIWC_LIVE", type=str)

    args = parser.parse_args()

    worker(args.macsy_settings, args.liwc_dict, args.blackboard)
