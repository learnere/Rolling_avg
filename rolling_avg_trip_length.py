# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     rolling_avg_trip_length
   Description :
   Author :        pzhang01
   date：          03/01/2020:
-------------------------------------------------
   Change Activity:
                   03/01/2020:
-------------------------------------------------
"""
__author__ = 'pzhang01'

import pandas as pd
from avg_trip_length import get_data, check_null


#
def time_series_index_by_col(dataframe, datetime_col_name):
    # set a column as time series index
    # eg :date_time_col_name ='pickup_datetim
    data[datetime_col_name]= pd.to_datetime(data[datetime_col_name])
    return data.set_index(datetime_col_name)


def ingest_data(ingest_data_file, old_data_file):
    old_data = pd.read_csv(old_data_file)
    ingest_data = pd.read_csv(ingest_data_file)
    s1 = old_data.dtypes
    s2 = ingest_data.dtypes
    if s1.equals(s2):
        # df to dict by row
        old_data_dict = old_data.to_dict('records')
        ingest_data_dict = ingest_data.to_dict('records')
        new_data = []
        for row_old in old_data_dict:
            dict_old = {}
            dict_old.update(row_old)
            new_data.append(dict_old)
        for row_ingest in ingest_data_dict:
            dict_ingest ={}
            dict_ingest.update(row_ingest)
            new_data.append(dict_ingest)
        return pd.DataFrame(new_data)



def get_avg_distance(dataframe):
    id_counts= data.groupby(['id','pickup_datetime'])['id'].size().reset_index(name='id_counts')
    sum_distance= data.groupby(['id','pickup_datetime'])['distance'].sum().reset_index(name='sum_distance')
    cols_to_use = sum_distance.columns.difference(id_counts.columns)
    # remove duplicate columns
    merged_df = pd.concat([sum_distance[cols_to_use],id_counts], axis=1, join='inner')
    avg_distance = pd.DataFrame(merged_df['sum_distance']/merged_df['id_counts'], columns=['avg_distance'])
    return merged_df.join(avg_distance)

def get_rolling_avg_by_days(avg_dataframe, rolling_day):
    day = str(rolling_day)
    return avg_dataframe.groupby('id').rolling(day+'D', on='pickup_datetime')['avg_distance'].mean().reset_index(name='SMA'+day)
    # min_periods = rolling_day (window size)

ingest_file = 'data/input/new_data.csv'
data_file = 'data/input/yellow_tripdata_2019-01.csv'

datetime_col = 'pickup_datetime'
data = get_data(data_file)
print(data[datetime_col].dtype)
time_series_data = time_series_index_by_col(data,datetime_col)
print(time_series_data)
print(time_series_data.index.dtype)
print('*')
print(time_series_data.dtypes)


result = (time_series_data.pipe(get_avg_distance)
              .pipe(get_rolling_avg_by_days, rolling_day = 2))
print(result)



