import unittest
import datetime
import pandas as pd
import pytest
from ...panel_constructors import (get_index_from_datastream)
from ...config import enabled_modules


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(3)

    def test_index_from_datastream(self):
        if 'qad' not in enabled_modules:
            pytest.skip("Skipping index from datastream test since 'qad' is not enabled")
        index_code_valid = 3670 #For FTSE 100
        index_name_valid = 'FTSE ALL SHARE INDEX'
        index_code_invalid = 1
        index_name_invalid = 'NotAnIndex'

        # Case with none of index_code or index_name
        self.assertRaises(ValueError, get_index_from_datastream, self.since, self.until, frequency='M')

        # Case with correct index_code and no index_name
        index_panel_ftse100 = get_index_from_datastream(self.since, self.until, frequency='M',
                                                        index_code=index_code_valid, keep_in_index_only=True)

        assert index_panel_ftse100.shape[0] > 0
        assert index_panel_ftse100[index_panel_ftse100['in_index'] == 0].shape[0] == 0

        # Case with correct index_name and no index_code
        index_panel_ftse_all_share = get_index_from_datastream(self.since, self.until, frequency='M',
                                                               index_name=index_name_valid)
        assert index_panel_ftse_all_share.shape[0] > 0

        # Case with correct index_name and index_code, but they do not correspond to each other
        index_panel_ftse100_2 = get_index_from_datastream(self.since, self.until, frequency='M', keep_in_index_only=True,
                                                          index_name=index_name_valid, index_code=index_code_valid)
        assert index_panel_ftse100.equals(index_panel_ftse100_2)

        # Case with correct index_name but invalid index_code
        self.assertRaises(KeyError, get_index_from_datastream, self.since, self.until,
                          frequency='M', index_name=index_name_valid, index_code=index_code_invalid)

        # Case with invalid index_name but correct index_code
        index_panel_invalid = get_index_from_datastream(self.since, self.until, frequency='M', keep_in_index_only=True,
                                                        index_name=index_name_invalid, index_code=index_code_valid)

        assert index_panel_ftse100.equals(index_panel_invalid)
