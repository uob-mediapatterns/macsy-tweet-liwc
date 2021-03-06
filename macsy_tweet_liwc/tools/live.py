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
import pymongo.errors
import logging
import io
import numpy as np
import csv
import sys


# even though flu isnt added yet, should just magically work
# when it is
chosen_indicator_names = ["affect","posemo","negemo","anx","anger","sad","flu"]

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


def worker(macsy_settings, liwc_dict, blackboard, checkpoint):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)


    chosen_indicator_idxs = np.isin([v for v,_ in liwc.categories.values()], chosen_indicator_names)

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
            # saves us having to create all the intermediate data, and then delete it
            doc["state"]["M"] = np.zeros(sum(chosen_indicator_idxs), dtype=np.float64)
            doc["state"]["k"] = 0
            doc["state"]["last_updated"] = oldest_possible

            doc["xs"] = []
            doc["ys"] = []

    oldest = min(doc["state"]["last_updated"] for doc in documents)
    # Only do tweets up until 30 minutes ago, so we can be sure they have settled
    # Alternative is to make a system which only updates as tweets are fully analysed etc, with some tag
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

    # Might want to rename this, too close to num_interval, or change num_interval to max_interval
    for doc in documents:
        doc["num_intervals"] = 0

    # NOTE Tweet isnt an object, it's a tuple
    # _id, vector, wc, wc_dic, values (last is only there if we ask for it)
    for (_id, vector, _, _, _) in pipeline(liwc, bbapi, filter):
        for doc in documents:
            if _id.generation_time >= doc["state"]["last_updated"]:
                start = floor_dt(doc["interval"], doc["state"]["last_updated"])
                end   = start + intervalToRelativeDelta(doc["interval"])
                while _id.generation_time >= end:
                    doc["num_intervals"] += 1

                    x = start
                    y = list(doc["state"]["M"])
                    # Add the volume to the end of the list
                    y.append(doc["state"]["k"])

                    doc["xs"] = doc["xs"][-doc["num_interval"]+1:] + [x]
                    doc["ys"] = doc["ys"][-doc["num_interval"]+1:] + [y]
                    doc["next_x"] = end

                    doc["state"]["M"] = np.zeros(sum(chosen_indicator_idxs), dtype=np.float64)
                    doc["state"]["k"] = 0
                    doc["state"]["last_updated"] = end

                    start = floor_dt(doc["interval"], doc["state"]["last_updated"])
                    end   = start + intervalToRelativeDelta(doc["interval"])

                    if checkpoint is not None and doc["num_intervals"] == checkpoint:
                        del doc["num_intervals"]

                        m = doc["state"]["M"]
                        doc["state"]["M"] = list(m)

                        while True:
                            try:
                                db[blackboard].find_one_and_replace({"_id": doc["_id"]}, doc)
                                break
                            except pymongo.errors.OperationFailure as e:
                                logging.exception(e)
                                logging.debug("Retrying")
                                continue

                        doc["state"]["M"] = m

                        doc["num_intervals"] = 0
                        logging.debug("checkpoint")

                # add tweet to state
                doc["state"]["k"] += 1
                
                # If we added or removed indicators since last time... truncate or pad. Not perfect if order is changed but what can you do
                # Only padding for now
                doc["state"]["M"] = np.pad(doc["state"]["M"], (0, sum(chosen_indicator_idxs) - len(doc["state"]["M"])), "constant")
                doc["state"]["M"] += (vector[chosen_indicator_idxs] - doc["state"]["M"]) / doc["state"]["k"]
                doc["state"]["last_updated"] = _id.generation_time
    
    # Moved inserting empty xs/ys into the client side, as a graph shift

    # There's no checking for failed saving here, although it shouldn't matter
    # too much as long as it works the next round
    # NOTE: Not true anymore. I don't know why I didn't bother before
    #       Makes more of a difference when you're computing a backlog
    #       and it fails (and doesnt get restarted)
    for doc in documents:
        doc["state"]["M"] = list(doc["state"]["M"])
        del doc["num_intervals"]

        while True:
            try:
                db[blackboard].find_one_and_replace({"_id": doc["_id"]}, doc)
                break
            except pymongo.errors.OperationFailure as e:
                logging.exception(e)
                logging.debug("Retrying")
                continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser("tweet-liwc-live", description="Pulls tweets from before 30 minutes ago from TWEET blackboard, processes them with LIWC, and takes period averages and places them into the target blackboard")
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("--blackboard", help="blackboard to store the series", nargs='?', const=1, default="TWEET_LIWC_LIVE", type=str)
    parser.add_argument("--checkpoint", help="store to mongo every n intervals", nargs='?', type=int)

    args = parser.parse_args()

    worker(args.macsy_settings, args.liwc_dict, args.blackboard, args.checkpoint)
