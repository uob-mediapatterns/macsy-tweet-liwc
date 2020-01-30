#!/bin/bash
# month - 3
# day + 2

ALL_LOCS="[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54]"

CREATE_DATASET="python -m macsy_tweet_liwc.tools.create_dataset LIWC2007.txt"

$CREATE_DATASET settings.json datasets/2017_election_uk_all.hdf5 TWEET "{ 'L': { '\$in': $ALL_LOCS } }" "2017-03-01" "2017-06-12T12:00:00.0000"
$CREATE_DATASET settings.json datasets/2015_election_uk_all.hdf5 TWEET "{ 'L': { '\$in': $ALL_LOCS } }" "2015-02-01" "2015-05-11T12:00:00.0000"
$CREATE_DATASET settings.json datasets/2010_election_uk_all.hdf5 TWEET "{ 'L': { '\$in': $ALL_LOCS } }" "2010-02-01" "2010-05-10T12:00:00.0000"
