import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from rolling_avg_trip_length import time_series_index_by_col,ingest_data


df = pd.DataFrame({'id':[1,2,3], 'distance':[2,3,7], 'pickup_datetime':['2019-01-01 00:46:40','2019-01-01 00:46:40','2019-01-01 00:46:40']})
df_same_datatype = pd.DataFrame({'id':[4,5,6], 'distance':[1,5,8], 'pickup_datetime':['2019-02-02 00:46:40','2019-02-02 00:46:40','2019-02-02 00:46:40']})
df_diff_datatype = pd.DataFrame({'id':[4,5,6], 'distance':[1.2,5,8], 'pickup_datetime':['2019-02-02 00:46:40','2019-02-02 00:46:40','2019-02-02 00:46:40']})

class MyTestCase(unittest.TestCase):
    #
    def test_time_series_data_type(self):
        datetime_col = 'pickup_datetime'
        time_series_data = time_series_index_by_col(df,datetime_col)
        self.assertEqual('datetime64[ns]',time_series_data.index.dtype)

    def test_ingest_data_same_type(self):
        result_df = pd.DataFrame({'id': [1, 2, 3, 4, 5, 6], 'distance': [2, 3, 7, 1, 5, 8], 'pickup_datetime': ['2019-01-01 00:46:40','2019-01-01 00:46:40','2019-01-01 00:46:40', '2019-02-02 00:46:40','2019-02-02 00:46:40','2019-02-02 00:46:40']})
        assert_frame_equal(result_df,ingest_data(df,df_same_datatype),check_like =True)

    def test_ingest_data_diff_type(self):
        new_data = ingest_data(df,df_diff_datatype)
        self.assertEqual(new_data,"Data types of the two inputs do not match")
if __name__ == '__main__':
    unittest.main()



