#!/usr/bin/env python

#----------------------------------------------------------------
# Author: Jason Gors <jasonDOTgorsATgmail>
# Creation Date: 10-31-2015
# Purpose:  graph data-structure for maintaining a tweet graph for 
#           handling new incoming tweets.
#----------------------------------------------------------------

from itertools import permutations


class TweetsGraph(object):
    """ Initalized without any arguments and returns an empty graph data- 
    structure.  The graph is then built and maintained by succesively  
    passing `Tweet` instances into the `update_graph` method. 
        eg. 
            tweet_graph = TweetsGraph()
            tweet = Tweet(created_at, hashtags)
            tweet_graph.update_graph(tweet)

        (see `Tweet` class doc for it's details)

        
    Methods
    -------
    update_graph: updates the graph by adding new hashtags as nodes and 
        their adjacent hashtags as neighboring nodes.


    Attributes
    ----------
    graph: dict where each key is a hashtag/node and each corresponding value  
        is a set of it's adjacent neighboring hashtags/nodes (eg. adjancey set)


    Returns
    -------
    empty graph


    Notes
    -----
    Each succesive tweet submitted into the graph (via the `update_graph` 
    method) adds the new tweets into the graph (if there are any) and 
    removes all of the graph's node neighbors that are older than 60 seconds 
    from the latest tweet."""


    def __init__(self):
        self.graph = {}
        self.__unique_timestamp_list = []
        self.__dict_of_timestamps_and_list_of_hashtag_sets = {}


    def __update_active_hashtag_nodes(self, current_timestamp, current_hashtags):
        '''This does all of the work in maintaining that only tweets that 
        are within 60 seconds prior to the current tweet are used in making up
        the hashtag graph.  This uses a first in, first out (fifo) convention for 
        maintaining that the newest timestamped hashtags are located at the end 
        of the list and the oldest hashtags at the beginning of the list. 

        Notes
        -----
        When each new tweet is passed in, a test checks whether it is timestamped  
        as more than 60s after the first/oldest tweet in the list, if so, then the    
        test checks whether the same holds true for the second tweet in the list, 
        and so on, until the test comes to a tweet that is within 60s of itself (or 
        comes to the end of he list), whereupon it then gets rid of all of the 
        tweets that are more than 60s away from this latest tweet; lastly, the
        newest tweet gets appended onto the end of the list.'''

        # potentially much smaller memory footprint on higher freq data -- this 
        # will scale much better if the tweet frequency is high.
        #
        # To track what should & shouldn't be included in the graph, this uses 
        # - a list of (unique) timestamps (fifo), along with
        # - a dict with a timestamp (for each sec) as the keys and with a list 
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
                    if (current_timestamp - old_timestamp) > 60.0:
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
            else:   # next_most_recent_timestamp == current_timestamp b/c tweets will be in sorted order
                # so don't need to append current_timestamp to __unique_timestamp_list 
                # b/c the current_timestamp isn't unique
                pass
        self.__manage_timestamps_and_hashtag_dict(current_hashtags, current_timestamp)


    def __manage_timestamps_and_hashtag_dict(self, current_hashtags, current_timestamp):
        ''' Determines what the state of the node/neighbor pairings are relative 
        to when their timestamp was'''

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
        '''If a hashtag node and neighbor pair is in the graph, 
        then this removes it.

        old_hashtags:  set of hashtags for an older tweet to be removed
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
        '''Delegation of building up and/or removing nodes from the 
        graph as they are passed into this method.

        Parameters
        ----------
        tweet:  an instance of class Tweet
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
            total_edge_len = 0
            for neighbors in self.graph.itervalues(): 
                total_edge_len += len(neighbors)
            avg_deg = total_edge_len / float(len(self.graph))
            return "{:.2f}".format(round(avg_deg, 2))
        else:
            return "{:.2f}".format(0.00)

