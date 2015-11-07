#!/usr/bin/env python

#----------------------------------------------------------------
# Author: Jason Gors <jasonDOTgorsATgmail>
# Creation Date: 10-31-2015
# Purpose:  `TweetsGraph` instances are used to create a graph data-structure, 
#           which can then be used to maintain a graph of adjacent tweets 
#           within a rolling time window. 
#----------------------------------------------------------------

from itertools import permutations


class TweetsGraph(object):
    """Initalized with a `time_window` argument (default: 60) and returns 
    an empty graph data-structure.  The graph can then be built and maintained 
    by succesively passing `Tweet` instances into the `update_graph` method. 
        eg. 
            tweet_graph = TweetsGraph()
            tweet = Tweet(created_at, hashtags)
            tweet_graph.update_graph(tweet)

        (see `Tweet` class in `tweet_processing.py` for it's details)

        
    Methods
    -------
    update_graph: updates a graph instance by adding all permutations of a 
        tweet's hashtag pairs as nodes and adjacent neighboring nodes.


    Attributes
    ----------
    graph: (dict) where each key is a node/hashtag and each corresponding value  
        is a set of it's adjacent neighboring nodes/hashtags (eg. adjacency set)

    time_window: (int) that specifies how long back (in seconds) old tweets should 
        remain in the graph before being removed (Default: 60)


    Returns
    -------
    empty graph data-structure


    Notes
    -----
    Each succesive tweet submitted into a graph instance (via the `update_graph` 
    method) adds the new tweet's hashtags into the graph (if there are any) 
    and removes all hashtag node neighbors (and any nodes themselves if they 
    are empty) that are older than the specified `time_window` when compared to 
    the latest tweet."""


    def __init__(self, time_window=60):
        self.graph = {}
        self.time_window = time_window
        self.__unique_timestamp_list = []
        self.__dict_of_timestamps_and_list_of_hashtag_sets = {}


    def __update_active_hashtag_nodes(self, current_timestamp, current_hashtags):
        '''This does all of the work in maintaining that only tweets that are  
        within the `time_window` prior to the current tweet are used in making up
        the hashtag graph.  This uses a first in, first out (fifo) ordering for 
        maintaining that the newest timestamped hashtags are located at the end 
        of the list and the oldest hashtag timestamps at the beginning of the list. 

        
        Parameters
        ----------
        current_timestamp:  epoch based time (int)
        current_hashtags:   (set) of hashtags for most recent tweet


        Notes
        -----
        When each new tweet is passed in, a test checks whether it is timestamped  
        as more than the `time_window` amount allowed after the first/oldest tweet 
        was passed into the list; if so, then the test checks whether the same holds 
        true for the second tweet in the list, and so on, until the test comes to a 
        tweet that is within the allowed `time_window` (or reaches the end of he list), 
        whereupon all tweets that are more than the allowed `time_window` away from 
        this latest tweet are removed; finally, the newest tweet gets appended onto 
        the end of the list so as to serve as the next most recent tweet for next time
        around.'''

        # To track what should & shouldn't be included in the graph, this uses 
        # - a list of (unique) timestamps (fifo), along with
        # - a dict with a timestamp (for each sec) as the keys and a list 
        # of corresponding hashtag sets as the values; like: 
        #
        # unique_timestamp_list = [timeOldest ... timeNewest]
        # dict_of_timestamps_and_lists_of_hashtag_sets = {timestamp_per_sec: [{hashtag_sets}]}
        #              eg  {1446679027: [{tweet6s_hashtags}, {tweet7s_hashtags}],
        #                   1446679021: [{tweet0s_hashtags}],
        #                   1446679029: [{tweet9s_hashtags}, {tweet10s_hashtags}],
        #                  }

        if not self.__unique_timestamp_list:
            self.__unique_timestamp_list.append(current_timestamp)
        else:
            next_most_recent_timestamp = self.__unique_timestamp_list[-1]
            if next_most_recent_timestamp < current_timestamp:
                for cnt, old_timestamp in enumerate(self.__unique_timestamp_list):
                    if (current_timestamp - old_timestamp) > self.time_window:
                        if old_timestamp in self.__dict_of_timestamps_and_list_of_hashtag_sets:
                            list_of_hashtag_sets = self.__dict_of_timestamps_and_list_of_hashtag_sets[old_timestamp]
                            for set_of_old_hashtags in list_of_hashtag_sets:
                                self.__remove_hashtags_from_graph(set_of_old_hashtags)
                            if not self.__dict_of_timestamps_and_list_of_hashtag_sets[old_timestamp]: # if only an empty set remains
                                del self.__dict_of_timestamps_and_list_of_hashtag_sets[old_timestamp]
                    else:
                        if cnt > 0: # don't recopy if we don't need to (eg. when cnt is 0)     
                            self.__unique_timestamp_list = self.__unique_timestamp_list[cnt:] 
                        break
                self.__unique_timestamp_list.append(current_timestamp)
            else:  # next_most_recent_timestamp == current_timestamp b/c tweets will 
                # be in sorted order, so don't need to append current_timestamp to 
                # __unique_timestamp_list b/c the current_timestamp isn't unique then 
                pass
        self.__manage_timestamps_and_hashtag_dict(current_hashtags, current_timestamp)


    def __manage_timestamps_and_hashtag_dict(self, current_hashtags, current_timestamp):
        '''Determines what the state of the node/neighbor pairings are relative 
        to when their timestamp occurred


        Parameters
        ----------
        current_hashtags:   (set) of hashtags for most recent tweet
        current_timestamp:  epoch based time (int)
        '''

        if current_timestamp in self.__dict_of_timestamps_and_list_of_hashtag_sets:
            if current_hashtags:
                self.__dict_of_timestamps_and_list_of_hashtag_sets[current_timestamp].append(current_hashtags)
            else:
                pass    # b/c we don't want to overwrite whatever might already be there
        else:
            if current_hashtags:
                self.__dict_of_timestamps_and_list_of_hashtag_sets[current_timestamp] = [current_hashtags]
            else:
                self.__dict_of_timestamps_and_list_of_hashtag_sets[current_timestamp] = [] 


    def __remove_hashtags_from_graph(self, old_hashtags):
        '''If an old hashtag node and neighbor pair is in the graph, then this removes it.


        Parameters
        ----------
        old_hashtags:  (set) of hashtags for an older tweet to be removed
        '''

        for ht1, ht2 in permutations(old_hashtags, 2):
            if ht1 in self.graph:
                # need this check b/c since using sets, no duplicates of a hashtag neighbor  
                # is stored under a hashtag_node; thus a given neighbor might have already of 
                # been removed in a previous removal step
                if ht2 in self.graph[ht1]:     
                    self.graph[ht1].remove(ht2)   # remove the hashtag_node from the neighbors set
                # if the hashtag_node doesn't have any more neighbors left, remove it from the graph.
                if not self.graph[ht1]:     
                    del self.graph[ht1]
            

    def update_graph(self, tweet):
        '''Delegation of building up and/or removing nodes from the graph 
        as they are passed into this method.


        Parameters
        ----------
        tweet:  an instance of class Tweet (see `tweet_processor.py`)
        '''
        current_hashtags = tweet.hashtags
        current_timestamp = tweet.timestamp
        if current_hashtags:
            # add all new pairs of hashtags into the graph
            for ht1, ht2 in permutations(current_hashtags, 2):
                if ht1 not in self.graph: 
                    self.graph[ht1] = {ht2}
                else:
                    self.graph[ht1].update({ht2})

        self.__update_active_hashtag_nodes(current_timestamp, current_hashtags)


    def get_graph_avg_degree_of_all_nodes(self):
        '''Calculates and returns the average degree for all nodes in all 
        graphs and subgraphs'''

        if self.graph:
            total_edge_len = sum([len(neighbors) for neighbors in self.graph.itervalues()]) 
            avg_deg = total_edge_len / float(len(self.graph))
            return "{:.2f}".format(round(avg_deg, 2))
        else:
            return "{:.2f}".format(0.00)

