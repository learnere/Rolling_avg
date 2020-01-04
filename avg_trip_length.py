# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     avg_trip_length
   Description :
   Author :        pzhang01
   date：          04/01/2020:
-------------------------------------------------
   Change Activity:
                   04/01/2020:
-------------------------------------------------
"""
__author__ = 'pzhang01'
import pandas as pd



#test functions

def get_id_counts_by_col(dataframe,col):
    return dataframe.groupby([col]).size().reset_index(name='id_counts')
def get_sum_dist_by_col(dataframe,col):
    return dataframe.groupby([col]).sum().unstack().reset_index(name='sum_distance')
def merge_idCounts_sumDist_by_col(id_counts, sum_dist, col):
    return pd.merge(id_counts,sum_dist, how='inner', on = col).drop(['level_0'], axis=1)
def avg_dist(merged_df):
    avg_distance = pd.DataFrame(merged_df['sum_distance']/merged_df['id_counts'], columns=['avg_distance'])
    return merged_df.join(avg_distance)
#

def get_data(file_path):
    id =[]
    distance = []
    pickup_datetime = []
    chunk_iter = pd.read_csv(file_path,chunksize=100000, usecols=['VendorID', 'trip_distance','tpep_pickup_datetime'])
    for chunk in chunk_iter:
        id.append(chunk['VendorID'])
        distance.append(chunk['trip_distance'])
        pickup_datetime.append(chunk['tpep_pickup_datetime'])
    vendor_id = pd.concat(id)
    trip_distance = pd.concat(distance)
    tpep_pickup_datetime = pd.concat(pickup_datetime)
    # rename columns
    data= pd.DataFrame({'id': vendor_id, 'distance':trip_distance, 'pickup_datetime': tpep_pickup_datetime})
    return data

def check_null(dataframe):
    '''check how many null data are there'''
    print('\n****check null value****\n')
    return print(dataframe.isnull().sum())

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

# test = 'data/test/test_input'
data_file = 'data/input/yellow_tripdata_2019-01.csv'

data = get_data(data_file)
check_null(data)
col = 'id'
print('result\n')
print(cal_avg_trip_length_by_col(data,col))
