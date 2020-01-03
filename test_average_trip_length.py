# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test
   Description :
   Author :        pzhang01
   date：          03/01/2020:
-------------------------------------------------
   Change Activity:
                   03/01/2020:
-------------------------------------------------
"""
__author__ = 'pzhang01'

import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from avg_trip_length import *



col = 'VendorID'

class DFTests(unittest.TestCase):

    def setUp(self):
        """ input data"""
        test_file_name = 'data/test/test_input'
        try:
            data = pd.read_csv(test_file_name)
            input_df = pd.DataFrame(data,columns=['VendorID', 'trip_distance','tpep_pickup_datetime'])
        except IOError:
            print('cannot open file')
        self.input_df = data

    def test_get_id_countsAsExpected(self):
        """ Test that the dataframe read in equals what you expect"""
        dict_id_counts ={'VendorID':[1,2,8], 'id_counts':[5,4,5]}
        id_counts = pd.DataFrame(dict_id_counts)
        assert_frame_equal(get_id_counts_by_col(self.input_df,'VendorID'),id_counts)

    def test_get_sum_distAsExpected(self):
        dict_sum_dist = {'VendorID':[1,2,8],'sum_distance':[10.2,20.5,9.1],'level_0':['trip_distance','trip_distance','trip_distance']}
        sum_dist = pd.DataFrame(dict_sum_dist)
        assert_frame_equal(get_sum_dist_by_col(self.input_df,'VendorID'),sum_dist)

    def test_get_mergeAsExpected(self):
        dict_merge = {'VendorID':[1,2,8],'sum_distance':[10.2,20.5,9.1],'id_counts':[5,4,5]}
        merge = pd.DataFrame(dict_merge)
        assert_frame_equal(merge_idCounts_sumDist_by_col(get_id_counts_by_col(self.input_df,col),get_sum_dist_by_col(self.input_df,col),col),merge)

    def test_get_avg_distAsExpected(self):
        dict_avg = {'VendorID':[1,2,8], 'id_counts':[5,4,5], 'sum_distance':[10.2, 20.5, 9.1], 'avg_distance': [2.040,5.125,1.820]}
        avg = pd.DataFrame(dict_avg)
        assert_frame_equal(get_sum_dist_by_col(self.input_df,col),avg)

    if __name__ =='__main__':
        unittest.main()

# data = pd.read_csv('data/test/test_input')
# input_df = pd.DataFrame(data,columns=['VendorID', 'trip_distance','tpep_pickup_datetime'])
# print(input_df)

#output
# dict_id_counts ={'VendorID':[1,2,8], 'id_counts':[5,4,5]}
# id_counts = pd.DataFrame(dict_id_counts)
#
# dict_sum_dist = {'VendorID':[1,2,8],'sum_distance':[10.2,20.5,9.1],'level_0':['trip_distance','trip_distance','trip_distance']}
# sum_dist = pd.DataFrame(dict_sum_dist)
#
# dict_merge = {'VendorID':[1,2,8],'sum_distance':[10.2,20.5,9.1],'id_counts':[5,4,5]}
# merge = pd.DataFrame(dict_merge)
#
# dict_avg = {'VendorID':[1,2,8], 'id_counts':[5,4,5], 'sum_distance':[10.2, 20.5, 9.1], 'avg_distance': [2.040,5.125,1.820]}
# avg = pd.DataFrame(dict_avg)
#
#
# col = 'VendorID'
# assert_frame_equal(get_id_counts_by_col(input_df,col),id_counts,check_like=True)
# assert_frame_equal(get_sum_dist_by_col(input_df,col),sum_dist,check_like=True)
# assert_frame_equal(merge_idCounts_sumDist_by_col(get_id_counts_by_col(input_df,col),get_sum_dist_by_col(input_df,col),col),merge,check_like=True)
# assert_frame_equal(cal_avg_trip_length_by_col(input_df,col),avg,check_like=True)


