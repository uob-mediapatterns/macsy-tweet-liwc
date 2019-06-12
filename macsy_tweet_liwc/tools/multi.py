import argparse
from macsy.api import BlackboardAPI
from liwc import LIWC
from macsy_tweet_liwc import pipeline
import dateutil.parser
from multiprocessing import Pool
import json
from bson.objectid import ObjectId
import logging
import io

def load(settings_path):
    with open(settings_path) as f:
        settings = json.load(f)

    bbapi = BlackboardAPI(settings)
    db = bbapi._BlackboardAPI__db

    return bbapi, db

def ranges(start_date, end_date, num_workers):
    delta = (end_date - start_date) / num_workers
    i = start_date
    for _ in range(num_workers):
        j = i + delta
        yield i.replace(minute=0, second=0, microsecond=0), j.replace(minute=0, second=0, microsecond=0)
        i = j

def worker(args):
    macsy_settings, liwc_dict, start_date, end_date = args

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    bbapi, db = load(macsy_settings)

    with io.open(liwc_dict, 'r', encoding="utf-8") as liwc_file:
        liwc = LIWC(liwc_file)

    filter = {
        "_id": {
            "$gte": ObjectId.from_datetime(start_date),
            "$lt": ObjectId.from_datetime(end_date)
        }
    }

    p = pipeline(liwc, bbapi, db, filter)

    while True:
        _id, total_tweets = p.send(None)
        logging.debug("{}: {} tweets".format(_id['x'].isoformat(), total_tweets))

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("liwc_dict", help="file containing LIWC dictionary", type=str)
    parser.add_argument("macsy_settings", help="file containing Macsy settings", type=str)
    parser.add_argument("num_workers", help="number of workers to run", type=int)
    parser.add_argument("start_date", help="start date", type=str)
    parser.add_argument("end_date", help="end date", type=str)

    # need to split a date range into n periods, rounded to the hour

    args = parser.parse_args()

    start_date = dateutil.parser.parse(args.start_date).replace(minute=0, second=0, microsecond=0)
    end_date = dateutil.parser.parse(args.end_date).replace(minute=0, second=0, microsecond=0)

    workers = list(ranges(start_date, end_date, args.num_workers))

    pool = Pool(args.num_workers) 
    pool.map(worker, [(args.macsy_settings, args.liwc_dict) + dates for dates in workers])
