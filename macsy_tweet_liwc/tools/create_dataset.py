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
import ast

# Just store the filename for macsy_settings
def worker(macsy_settings, filename, blackboard, filter, liwc_dict, trim_rt, indicator_resolution, start_date, end_date):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    earliest = start_date.replace(second=0, microsecond=0)
    latest   = end_date.replace(second=0, microsecond=0)

    times = [dt.replace(tzinfo=tz.UTC) for dt in rrule.rrule(rrule.MINUTELY, dtstart=start_date.replace(second=0, microsecond=0), until=end_date.replace(second=0, microsecond=0))]

    f = h5py.File(filename, "w")

    tweets = f.create_group("tweets")

    tweets.attrs["macsy_settings"] = macsy_settings
    tweets.attrs["blackboard"] = blackboard

    # Array to hold the start and end dates for which this has been inserted
    tweets.attrs["inserted"] = np.empty((0,), dtype=h5py.string_dtype())

    # Test the filter works and insert
    ast.literal_eval(filter)
    tweets.attrs["filter"] = filter

    tweets.attrs["trim_rt"] = trim_rt

    tweets.attrs["indicator_resolution"] = indicator_resolution

    tweets.attrs["liwc_dict"] = liwc_dict

    tweets.create_dataset("times", data=np.asarray([t.isoformat() for t in times], dtype=h5py.string_dtype()))
    tweets.create_dataset("wordcounts", (len(times),), dtype=np.float64)
    tweets.create_dataset("tweetcounts", (len(times),), dtype=np.float64)

    if indicator_resolution == "category":
        indicators = tweets.create_dataset("indicators", (len(times), len(liwc.categories)), dtype=np.float64)
        labels = tweets.create_dataset("labels", data=np.asarray([v for v,_ in liwc.categories.values()], dtype=h5py.string_dtype()))
    elif indicator_resolution == "rule":
        rule_liwc = CountRules(liwc)
        indicators = tweets.create_dataset("indicators", (len(times), len(rule_liwc.rules)), dtype=np.float64)
        labels = tweets.create_dataset("labels", data=np.asarray(rule_liwc.rules, dtype=h5py.string_dtype()))

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("hdf5", help="dataset file", type=str)
    parser.add_argument("blackboard", help="blackboard", type=str)
    parser.add_argument("filter", help="filter", type=str)
    parser.add_argument("start_date", help="start date", type=str)
    parser.add_argument("end_date", help="end date", type=str)
    parser.add_argument("--trim-rt", type=str2bool, nargs="?", const=False, default=False, help="Trim the text RT that appears in front of retweets")
    parser.add_argument("--indicator-resolution", choices=["category", "rule"], help="Either rule or category. Defaults to category", default="category", type=str)

    args = parser.parse_args()

    # Remember these are TZ Naive
    start_date = dtparse(args.start_date)
    end_date = dtparse(args.end_date)

    worker(args.macsy_settings, args.hdf5, args.blackboard, args.filter, args.liwc_dict, args.trim_rt, args.indicator_resolution, start_date, end_date)
