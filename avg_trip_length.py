# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     avg_trip_length
   Description :   calculate the average trip length
   Author :        pzhang01
   date：          02/01/2020:
-------------------------------------------------
   Change Activity:
                   02/01/2020:
-------------------------------------------------
"""
__author__ = 'pzhang01'
import pandas as pd


# test_file = 'data/input/yello_data.csv'
# file = 'data/input/yellow_tripdata_2019-01.csv'
# ingest_file = 'data/input/new_data.csv'
test = 'data/test/test_input'

def get_id_counts_by_col(dataframe,col):
    return dataframe.groupby([col]).size().reset_index(name='id_counts')
def get_sum_dist_by_col(dataframe,col):
    return dataframe.groupby([col]).sum().unstack().reset_index(name='sum_distance')
def merge_idCounts_sumDist_by_col(id_counts, sum_dist, col):
    return pd.merge(id_counts,sum_dist, how='inner', on = col).drop(['level_0'], axis=1)

def avg_dist(merged_df):
    avg_distance = pd.DataFrame(merged_df['sum_distance']/merged_df['id_counts'], columns=['avg_distance'])
    return merged_df.join(avg_distance)


def cal_avg_trip_length_by_col(dataframe,col):
    id_counts = dataframe.groupby([col]).size().reset_index(name='id_counts')
    #group the data by the given column and gets the count of rows per group
    sum_dist = dataframe.groupby([col]).sum().unstack().reset_index(name='sum_distance')
    #group the data by the given column and gets the sum of distance per group
    merged_df = pd.merge(id_counts,sum_dist, how='inner', on = col).drop(['level_0'], axis=1)
    # inner join the previous results into one DataFrame and drop useless column named 'level_0'
    avg_distance = pd.DataFrame(merged_df['sum_distance']/merged_df['id_counts'], columns=['avg_distance'])
    # calculate the average distance
    return merged_df.join(avg_distance)

def check_null(dataframe):
    '''check how many null data are there'''
    print('\n****check null value****\n','The number of null value = ')
    return dataframe.isnull().sum()

id =[]
distance = []
pickup_datetime = []
# dtype={'VendorID': 'int64', 'trip_distance': 'float64','tpep_pickup_datetime':'object'},
chunk_iter = pd.read_csv(test,chunksize=100000, usecols=['VendorID', 'trip_distance','tpep_pickup_datetime'])
for chunk in chunk_iter:
    id.append(chunk['VendorID'])
    distance.append(chunk['trip_distance'])
    pickup_datetime.append(chunk['tpep_pickup_datetime'])
vendor_id = pd.concat(id)
trip_distance = pd.concat(distance)
tpep_pickup_datetime = pd.concat(pickup_datetime)
data= pd.DataFrame({'id': vendor_id, 'distance':trip_distance, 'pickup_datetime': tpep_pickup_datetime})
print(data)
print('*')
# data.set_index('pickup_datetime')
print(cal_avg_trip_length_by_col(data,'id'))

col ='id'
# print('id_counts')
# id_counts = get_id_counts_by_col(data,col)
# print(id_counts)
#
# print('sum_dist')
# sum_dist = get_sum_dist_by_col(data,col)
# print(sum_dist)
# print(sum_dist.shape)
#
# print('merge')
# merge = merge_idCounts_sumDist_by_col(id_counts,sum_dist,col)
# print(merge)









