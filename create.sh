#!/bin/bash
START="2019-9-1"
END="2019-12-14"

ALL_LOCS="[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54]"

CREATE_DATASET="python -m macsy_tweet_liwc.tools.create_dataset LIWC2007.txt"

$CREATE_DATASET settings.json datasets/uk_all.hdf5 TWEET "{ 'L': { '\$in': $ALL_LOCS } }" $START $END

$CREATE_DATASET settings.s11.json datasets/leavers.hdf5 TWEET_PIPE_B "{ 'Tg': 75750 }" $START $END
$CREATE_DATASET settings.s11.json datasets/remainers.hdf5 TWEET_PIPE_B "{ 'Tg': 75751 }" $START $END

$CREATE_DATASET settings.json datasets/uk_female.hdf5 TWEET "{ 'G': 'f', 'L': { '\$in': $ALL_LOCS } }" $START $END
$CREATE_DATASET settings.json datasets/uk_male.hdf5 TWEET "{ 'G': 'm', 'L': { '\$in': $ALL_LOCS } }" $START $END


#{ "Ctrl" : 0, "Nm" : "SourceSRCH>Welsh", "_id" : 76399 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>BrexitParty", "_id" : 76401 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>Green", "_id" : 76402 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>UKIP", "_id" : 76403 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>Conservatives", "_id" : 76404 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>SNP", "_id" : 76405 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>LibDem", "_id" : 76406 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>DUP", "_id" : 76407 }
#{ "Ctrl" : 0, "Nm" : "SourceSRCH>Labour", "_id" : 76408 }

#{ "Ctrl" : 0, "Nm" : "SourceSRCH>Combined", "_id" : 76400 }

#{ "Ctrl" : 0, "Nm" : "SourceHT>ge2019", "_id" : 75761 }
#{ "Ctrl" : 0, "Nm" : "SourceHT>ge19", "_id" : 75762 }
#{ "Ctrl" : 0, "Nm" : "SourceHT>generalelection", "_id" : 75763 }
#{ "Ctrl" : 0, "Nm" : "SourceHT>generalelection19", "_id" : 75765 }
#{ "Ctrl" : 0, "Nm" : "SourceHT>generalelection2019", "_id" : 75764 }


$CREATE_DATASET settings.s11.json datasets/welsh.hdf5 TWEET_PIPE_B "{ 'Tg': 76399 }" $START $END
$CREATE_DATASET settings.s11.json datasets/brexitparty.hdf5 TWEET_PIPE_B "{ 'Tg': 76401 }" $START $END
$CREATE_DATASET settings.s11.json datasets/green.hdf5 TWEET_PIPE_B "{ 'Tg': 76402 }" $START $END
$CREATE_DATASET settings.s11.json datasets/ukip.hdf5 TWEET_PIPE_B "{ 'Tg': 76403 }" $START $END
$CREATE_DATASET settings.s11.json datasets/conservatives.hdf5 TWEET_PIPE_B "{ 'Tg': 76404 }" $START $END
$CREATE_DATASET settings.s11.json datasets/snp.hdf5 TWEET_PIPE_B "{ 'Tg': 76405 }" $START $END
$CREATE_DATASET settings.s11.json datasets/libdem.hdf5 TWEET_PIPE_B "{ 'Tg': 76406 }" $START $END
$CREATE_DATASET settings.s11.json datasets/dup.hdf5 TWEET_PIPE_B "{ 'Tg': 76407 }" $START $END
$CREATE_DATASET settings.s11.json datasets/labour.hdf5 TWEET_PIPE_B "{ 'Tg': 76408 }" $START $END

$CREATE_DATASET settings.s11.json datasets/combined_parties.hdf5 TWEET_PIPE_B "{ 'Tg': 76400 }" $START $END

$CREATE_DATASET settings.s11.json datasets/ge2019.hdf5 TWEET_PIPE_B "{ 'Tg': { '\$in': [ 75761, 75762, 75763, 75765, 75764 ] } }" $START $END


# Do the main ones like combined #GE's
# Individual parties
# Combined parties

# Also manually collect what was crawled via the logs or module_run
