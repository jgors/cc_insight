#!/usr/bin/env python

#----------------------------------------------------------------
# Author: Jason Gors <jasonDOTgorsATgmail>
# Creation Date: 10-31-2015
# Purpose:  creates an interface for new incoming tweets via the Tweet 
#           class where it cleans incoming tweets and generates a timestamp
#           so the tweet can be passed into a TweetsGraph instance
#----------------------------------------------------------------
import os
from os.path import abspath
from os.path import join as pjoin
import sys
from json import loads as json_loads 
from string import translate
from string import maketrans
from codecs import encode as codecs_encode
import codecs 
from time import strptime
from calendar import timegm

from tweets_graph import TweetsGraph


unicode_tweets_count = 0


def clean_text(text_to_convert, count_unicode=False):
    '''Removes unicode and escape chars from passed in text_to_convert

    Parameters
    ----------
    text_to_convert: str to normalize 
    count_unicode:  either True (for tweet text) or False (for hashtags)

    Returns
    -------
    an ascii str without unicode and escape chars
    
    Refer to for more details on tweets: 
    https://dev.twitter.com/streaming/overview/processing
    '''

    try:
        ascii_txt = codecs_encode(text_to_convert, 'ascii')
    except UnicodeEncodeError:
        if count_unicode:
            global unicode_tweets_count  # works, but is hacky 
            unicode_tweets_count += 1

        # Remove all non ASCII characters from the unicode string
        ascii_txt = codecs_encode(text_to_convert, 'ascii', 'ignore')
        # ascii_txt = str(''.join([c for c in text if ord(c) < 128])) # same
    
    
    ### first replace the whitespace chars with space
    white_space_chars = "\n\t\r"
    whitespace_replacements = ' ' * len(white_space_chars)
    trans_table = maketrans(white_space_chars, whitespace_replacements)
    ws_replaced_txt = translate(ascii_txt, trans_table)
    # trans_table = maketrans(table_in, table_out)

    # return ws_replaced_txt 

    chars_to_remove = ''.join([chr(char) for char in range(0,32) + range(128,256)])
    escaped_txt = translate(ws_replaced_txt, None, chars_to_remove)
    # ''.join([chr(char) for char in xrange(32,127)])   # we want to keep these

    return escaped_txt 


class Tweet(object):
    ''' Used to create a tweet instance to pass into a TweetsGraph.

    Attributes
    ----------
    timestamp: takes tweet `created_at` field and converts to epoch based time; 
        used to better perform time based comparisons and manipulations.  

    hashtags: takes list of a tweet's hashtags, where unicode and 
        escape chars are removed.  Will either return empty set or a 
        set of length >= 2'''


    def __init__(self, created_at, hashtags):
        '''
        Parameters
        ----------
        created_at: `created_at` field str obtained from the Twitter api json output
        hashtags:   list of hashtags strs obtained from the Twitter's api json output
        '''

        self.timestamp = timegm(strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")) 
        
        hashtags_cleaned = [clean_text(hashtag).lower() for hashtag in hashtags]
        hashtags_to_use = set([ht for ht in hashtags_cleaned if hashtag])
        hashtags_for_graph = hashtags_to_use if len(hashtags_to_use) >= 2 else set()
        self.hashtags = hashtags_for_graph



if __name__ == '__main__':

    if sys.argv[1:]:
        # test other datasets by providing a path as single arg to script
        tweets_data_incoming = sys.argv[1]
        assert os.path.isfile(tweets_data_incoming), "Error: need to pass in a data file that exists."
        tweets_incomming_path = abspath(tweets_data_incoming)
    else:
        # for the actual test data set 
        # tweets_incomming_path = abspath(pjoin(os.pardir, 'tweet_input', 'tweets.txt'))     
        tweets_incomming_path = abspath(pjoin('tweet_input', 'tweets.txt'))     
    # tweet_output_path = pjoin(os.pardir, 'tweet_output')
    tweet_output_path = pjoin('tweet_output')

    tweet_graph = TweetsGraph()
    ft1 = open(abspath(pjoin(tweet_output_path, 'ft1.txt')), 'w')
    ft2 = open(abspath(pjoin(tweet_output_path, 'ft2.txt')), 'w')
    close_files = lambda l: [f.close() for f in l]

    with codecs.open(tweets_incomming_path, 'r', encoding='utf-8', errors='ignore') as tweets_incomming:  
        # NOTE all tweets from the api are utf-8 encoded:
        # https://dev.twitter.com/overview/api/counting-characters
        for cnt, tweet in enumerate(tweets_incomming, start=1):
            tweet_dict =  json_loads(tweet)
            
            if 'limit' in tweet_dict:
                continue
            try:
                text = tweet_dict["text"]
                created_at = tweet_dict["created_at"]
                hashtags = [hashtag['text'] for hashtag in tweet_dict['entities']['hashtags']]
                tweet = Tweet(created_at, hashtags) 
                tweet_graph.update_graph(tweet)

                cleaned_text = clean_text(text, count_unicode=True)
                # print 'tweet_cnt: {}, num_graph_nodes: {}, avg_deg: {}'.format(cnt, len(tweet_graph.graph), tweet_graph.get_graph_avg_degree_of_all_nodes())
                ft1.write('{} (timestamp: {})\n'.format(cleaned_text, created_at))
                ft2.write('{}\n'.format(tweet_graph.get_graph_avg_degree_of_all_nodes()))

            except:     # sort of an anti-pattern to exception handle on main like this, but I want to play it safe.
                # TODO write to logger that this tweet failed
                # print "Tweet on ln {} of file failed in the pipeline".format(cnt)
                pass

        ft1.write('\n{} tweets contained unicode.'.format(unicode_tweets_count))
    close_files([ft1, ft2])
