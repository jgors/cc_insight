#!/usr/bin/env python

#----------------------------------------------------------------
# Author: Jason Gors <jasonDOTgorsATgmail>
# Creation Date: 10-31-2015
# Purpose:  Creates an interface for incoming tweets via the `Tweet` class,  
#           which standardizes the tweet's text and generates a timestamp 
#           so that the tweet can then be passed into a `TweetsGraph`
#           instance (which is based on a rolling time window).
#----------------------------------------------------------------
import os
from os import path
import sys
import logging
from json import loads as json_loads 
from string import translate
from string import maketrans
from codecs import encode as codecs_encode
from time import strptime
from calendar import timegm

from tweets_graph import TweetsGraph


# FAQ says, "all whitespace escape characters should be replaced with a single space"
#   And the C language defines whitespace characters to be: 
#   """...space (" "), horizontal tab ("\t"), new-line ("\n"), 
#      vertical tab ("\v"), and form-feed ("\f")  -- from [1] and [2]. so i'll use those
# [1] https://en.wikipedia.org/wiki/Whitespace_character#Programming_languages
chars_to_replace_with_space = {"\t", "\n", "\r", "\v", "\f"}  #also there is str.isspace()
white_space_chars = "\t\n\r\v\f"
whitespace_replacements = ' ' * len(white_space_chars)
# ws_replaced_txt = ''.join([' ' if char in chars_to_replace_with_space 
                                # else char for char in ascii_txt])
trans_table = maketrans(white_space_chars, whitespace_replacements)

# Also, FAQ says only keep ascii chars 32-127 
# set_of_ascii_we_want_to_keep = set([chr(char) for char in xrange(32,127)])  

unicode_tweets_count = 0


def clean_text(text_to_convert, count_unicode=False):
    '''Removes unicode and escape chars from text_to_convert;
    also counts the number of tweets which contained unicode. 

    Parameters
    ----------
    text_to_convert: (str) to standardize 
    count_unicode:  either (True) for tweet text or (False) for 
        a tweet hashtag


    Returns
    -------
    an ascii (str) without unicode and escape chars

    
    Refer to here for more details on tweet formating: 
    https://dev.twitter.com/streaming/overview/processing
    '''
            
    try:
        # https://docs.python.org/2/howto/unicode.html
        ascii_txt = codecs_encode(text_to_convert, 'ascii')
    except UnicodeEncodeError:
        if count_unicode:
            global unicode_tweets_count  # fine for now
            unicode_tweets_count += 1

        # remove all non ascii chars from the unicode string
        ascii_txt = codecs_encode(text_to_convert, 'ascii', 'ignore')

    ### replace the whitespace chars with a single space
    ws_replaced_txt = translate(ascii_txt, trans_table)

    return ws_replaced_txt 



class Tweet(object):
    '''Holds relevant information from a tweet so that class instances 
    can be passed into a TweetsGraph instance (to build a hashtag graph).


    Attributes
    ----------
    timestamp: takes tweet `created_at` (str) field and converts to an 
        epoch based time format; used to better perform time based 
        comparisons and manipulations.

    hashtags: takes a (list) of a tweet's hashtags and returns them with 
        unicode and escape chars removed.  Will either return an empty 
        set or a set of length >= 2'''


    def __init__(self, created_at, hashtags):
        '''
        Parameters
        ----------
        created_at: `created_at` field (str) obtained from the Twitter api json output
        hashtags:   (list) of hashtags (strs) obtained from the Twitter api json output
        '''

        self.timestamp = timegm(strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")) 
        
        hashtags_cleaned = [clean_text(hashtag).lower() for hashtag in hashtags]
        hashtags_to_use = set([ht for ht in hashtags_cleaned if hashtag])
        hashtags_for_graph = hashtags_to_use if len(hashtags_to_use) >= 2 else set()
        self.hashtags = hashtags_for_graph



if __name__ == '__main__':

    if sys.argv[1:]:
        # test other datasets by providing a path as single arg to this script
        tweets_data_incoming = sys.argv[1]
        assert path.isfile(tweets_data_incoming), "Error: need to pass in a data file that exists."
        tweets_incomming_path = path.abspath(tweets_data_incoming)
    else:
        # to run the actual test data set (needs to be run from root of repo!) 
        tweets_incomming_path = path.abspath(path.join('tweet_input', 'tweets.txt'))     
    tweet_output_path = path.abspath('tweet_output')

    if not path.isdir(tweet_output_path):
        os.makedirs(tweet_output_path)

    ft1 = open(path.abspath(path.join(tweet_output_path, 'ft1.txt')), 'w')
    ft2 = open(path.abspath(path.join(tweet_output_path, 'ft2.txt')), 'w')
    close_files = lambda l: [f.close() for f in l]

    tweet_graph = TweetsGraph(time_window=60)

    with open(tweets_incomming_path, 'r') as tweets_incomming:  
        # all tweets from the api are utf-8 encoded:
        # https://dev.twitter.com/overview/api/counting-characters
        for cnt, tweet in enumerate(tweets_incomming, start=1):
            try:
                tweet_dict =  json_loads(tweet)     # json.loads uses utf-8 decoding by default
                text = tweet_dict["text"]
                created_at = tweet_dict["created_at"]
                hashtags = [hashtag['text'] for hashtag in tweet_dict['entities']['hashtags']]
                tweet = Tweet(created_at, hashtags) 
                tweet_graph.update_graph(tweet)

                cleaned_text = clean_text(text, count_unicode=True)
                # logging.debug('tweet_cnt: {}, num_graph_nodes: {}, avg_deg: {}'.format(
                              # cnt, len(tweet_graph.graph), tweet_graph.get_graph_avg_degree_of_all_nodes()))
                # print 'tweet_cnt: {}, num_graph_nodes: {}, avg_deg: {}'.format(cnt, len(tweet_graph.graph), tweet_graph.get_graph_avg_degree_of_all_nodes())
                ft1.write('{} (timestamp: {})\n'.format(cleaned_text, created_at))
                ft2.write('{}\n'.format(tweet_graph.get_graph_avg_degree_of_all_nodes()))

            except Exception as e:  #  don't normally exception handle in main like this, but play it safe on unknown data.
                # logging.exception("Tweet on ln {} failed work.  Exception {}".format(cnt, e))
                pass

        ft1.write('\n{} tweets contained unicode.'.format(unicode_tweets_count))
    close_files([ft1, ft2])
