import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from rolling_avg_trip_length import time_series_index_by_col

col = 'VendorID'
test_file_name = 'data/test/test_input'
data = pd.read_csv(test_file_name)
input_df = pd.DataFrame(data,columns=['VendorID', 'trip_distance','tpep_pickup_datetime'])


class MyTestCase(unittest.TestCase):

    def test_time_series_data_type(self):
        datetime_col = 'pickup_datetime'
        time_series_data = time_series_index_by_col(data,datetime_col)
        self.assertEqual('datetime64[ns]',time_series_data.index.dtype)

if __name__ == '__main__':
    unittest.main()



