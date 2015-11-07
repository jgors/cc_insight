#!/usr/bin/env bash

# To execute, just run this script directly with:
#   $ sh ./run.sh
# 
# Or this:
#   $ python ./src/tweet_processor.py
#
# Optionally, the script can take one argument -- the path to a data set file.
#   $ python ./src/tweet_processor.py ./data-gen/tweets.txt


python ./src/tweet_processor.py

# pypy is WAY faster!
#pypy ./src/tweet_processor.py
