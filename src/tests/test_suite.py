#!/usr/bin/env python

#----------------------------------------------------------------
# Author: Jason Gors <jasonDOTgorsATgmail>
# Creation Date: 11-03-2015
# Purpose:
#----------------------------------------------------------------

import sys
import os
import json

repo_root = os.getcwd()
sys.path.insert(0, os.path.abspath(repo_root))
from tweet_processor import Tweet
from tweet_processor import clean_text
from tweets_graph import TweetsGraph 
from nose.tools import ok_ 

tests_dir = os.path.join(repo_root, 'src', 'tests')


class TestTweet(object):

    def test_tweet(self):
        tweet = Tweet('Thu Oct 29 17:51:01 +0000 2015', hashtags=[])
        ok_(tweet.hashtags == set())
        ok_(tweet.timestamp == 1446141061) 

        # not sure if this ever happens, but just to be safe
        tweet = Tweet('Thu Oct 29 17:51:00 +0000 2015', hashtags=['Spark', 'Apache'])
        ok_(tweet.hashtags == set(['spark', 'apache']))
        ok_(tweet.timestamp == 1446141060)


class TestTweetsGraph(object):
    
    def setUp(self):
        self.tweet_graph = TweetsGraph()
        ok_(self.tweet_graph.graph == {})


    def test_update_graph_with_example_from_instructions_manually(self):
        '''exact example from the online instructions done manually here: 
        https://github.com/InsightDataScience/coding-challenge#building-the-twitter-hashtag-graph'''

        
        # First tweet added to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:51:01 +0000 2015', hashtags=['Spark', 'Apache'])) 
        # graph will have each hashtag as a node and neighbor to one another 
        assert self.tweet_graph.graph == {'apache': set(['spark']), 
                                           'spark': set(['apache'])}
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '1.00'


        # Second tweet added to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:51:30 +0000 2015', hashtags=['Apache', 'Hadoop', 'Storm'])) 
        # graph gets updated
        assert self.tweet_graph.graph == {'apache': set(['spark', 'hadoop', 'storm']), 
                                           'spark': set(['apache']), 
                                           'hadoop': set(['apache', 'storm']), 
                                           'storm': set(['apache', 'hadoop'])}  
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '2.00' 


        # Third tweet to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:51:55 +0000 2015', hashtags=['Apache'])) 
        # graph stays unchanged since there was only one hashtag passed in for this tweet
        assert self.tweet_graph.graph == {'apache': set(['spark', 'hadoop', 'storm']), 
                                           'spark': set(['apache']), 
                                           'hadoop': set(['apache', 'storm']), 
                                           'storm': set(['apache', 'hadoop'])}
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '2.00' 


        # Fourth tweet to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:51:56 +0000 2015', hashtags=['Flink', 'Spark'])) 
        # graph gets updated accordingly
        assert self.tweet_graph.graph == {'apache': set(['spark', 'hadoop', 'storm']), 
                                           'spark': set(['apache', 'flink']), 
                                           'flink': set(['spark']), 
                                           'hadoop': set(['apache', 'storm']), 
                                           'storm': set(['apache', 'hadoop'])}
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '2.00' 


        # Fifth tweet to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:51:59 +0000 2015', hashtags=['HBase', 'Spark'])) 
        # graph gets updated 
        assert self.tweet_graph.graph == {'flink': set(['spark']), 
                                           'hadoop': set(['apache', 'storm']), 
                                           'storm': set(['apache', 'hadoop']), 
                                           'apache': set(['spark', 'hadoop', 'storm']), 
                                           'hbase': set(['spark']), 
                                           'spark': set(['apache', 'hbase', 'flink'])}
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '2.00' 


        # Last tweet to the graph
        self.tweet_graph.update_graph(Tweet('Thu Oct 29 17:52:05 +0000 2015', hashtags=['Apache'])) 
        # graph gets updated and now the Spark and Apache edge is remove b/c the tweet with them in it was older than 60s 
        assert self.tweet_graph.graph == {'flink': set(['spark']), 
                                           'hadoop': set(['apache', 'storm']), 
                                           'storm': set(['apache', 'hadoop']), 
                                           'apache': set(['hadoop', 'storm']), 
                                           'hbase': set(['spark']), 
                                           'spark': set(['hbase', 'flink'])}
        # get the graph avg degree 
        assert self.tweet_graph.get_graph_avg_degree_of_all_nodes() == '1.67' 
        
       
        # now test this against graph we just did manually with the same data loaded (also from the instructions 
        tweets_test_graph2 = TweetsGraph()
        testfile = os.path.join(tests_dir, 'test_data', 'data_for_building_hashtag_graph.txt')
        with open(testfile, 'r') as f: 
            for tweet in f:
                tweet_dict = json.loads(tweet)
                hashtags = [hashtag['text'] for hashtag in tweet_dict['entities']['hashtags']]
                tweets_test_graph2.update_graph(Tweet(tweet_dict['created_at'], hashtags))
        # check that the graph output here is the same as for the previous example that was just performed manually  
        assert tweets_test_graph2.graph == self.tweet_graph.graph 


def check_clean_text(testfile, tweet1_correct, tweet2_correct):

    with open(testfile, 'r') as testfile: 
        for cnt, tweet in enumerate(testfile, start=1):
            tweet_dict = json.loads(tweet)
            tweet_text = tweet_dict['text'] 

            if cnt == 1:    # the first tweet is ascii
                cleaned_txt = clean_text(tweet_text, count_unicode=False)
                ok_(cleaned_txt == tweet1_correct)
            elif cnt == 2:  # the second tweet is unicode
                cleaned_txt = clean_text(tweet_text, count_unicode=True)
                ok_(cleaned_txt == tweet2_correct)


def test_clean_text():
    
    testfile1 = os.path.join(tests_dir, 'test_data', 'data_from_instructions_orig.txt')
    tweet1_correct = "Spark Summit East this week! #Spark #Apache"
    tweet2_correct = "I'm at Terminal de Integrao do Varadouro in Joo Pessoa, PB https://t.co/HOl34REL1a"
    yield check_clean_text, testfile1, tweet1_correct, tweet2_correct

    testfile2 = os.path.join(tests_dir, 'test_data', 'data_from_instructions_modified_with_more_esc_chars.txt')
    tweet1_correct = "Spark/ Summit\ East this  week! #Spark  #Apache"
    tweet2_correct = "I'm at /Terminal\" de\ Integrao do  Varadouro in Joo Pessoa, PB  https://t.co/HOl34REL1a"
    yield check_clean_text, testfile2, tweet1_correct, tweet2_correct
