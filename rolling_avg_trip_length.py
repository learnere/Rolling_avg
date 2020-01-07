# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     rolling_avg_trip_length
   Description :   #https://wiki.python.org/moin/TimeComplexity
   Author :        pzhang01
   date：          03/01/2020:
-------------------------------------------------
   Change Activity:
                   03/01/2020:
-------------------------------------------------
"""
__author__ = 'pzhang01'

import pandas as pd
import numpy as np
import time
from avg_trip_length import get_data


#
def time_series_index_by_col(dataframe, datetime_col_name):
    # set a column as time series index
    # eg :date_time_col_name ='pickup_datetim
    dataframe[datetime_col_name]= pd.to_datetime(dataframe[datetime_col_name])
    return dataframe.set_index(datetime_col_name)


def ingest_data(old_dataframe, ingest_data_frame):
    s1 = old_dataframe.head(5).dtypes
    s2 = ingest_data_frame.head(5).dtypes
    if s1.equals(s2):
        old_data_dict = old_dataframe.to_dict('list')
        ingest_data_dict = ingest_data_frame.to_dict('list')
        id_list = old_data_dict['id']
        distance_list = old_data_dict['distance']
        time_list = old_data_dict['pickup_datetime']
        for i in ingest_data_dict['id']:
            id_list.append(i)
        for j in ingest_data_dict['distance']:
            distance_list.append(j)
        for k in ingest_data_dict['pickup_datetime']:
            time_list.append(k)
        new_data = {'id':id_list,'distance':distance_list, 'pickup_datetime':time_list}
        return pd.DataFrame.from_dict(new_data)
    else:
        return "Data types of the two inputs do not match"



def get_avg_distance(dataframe):
    id_counts= dataframe.groupby(['id','pickup_datetime'])['id'].size().reset_index(name='id_counts')
    sum_distance= dataframe.groupby(['id','pickup_datetime'])['distance'].sum().reset_index(name='sum_distance')
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
test = 'data/test/test_input'
datetime_col = 'pickup_datetime'

data = get_data(data_file)

new_data = get_data(ingest_file)


# start_time=time.time()
result = (data.pipe(ingest_data,ingest_data_frame = new_data)
              .pipe(time_series_index_by_col,datetime_col)
              .pipe(get_avg_distance)
              .pipe(get_rolling_avg_by_days, rolling_day = 45))

print(result)
# end_time=time.time()
# print('Pipeline execution time = %.6f seconds' % (end_time-start_time))


