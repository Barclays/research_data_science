import logging
import unittest
import datetime
import pandas as pd

from ....panel_constructors import (get_sp_500_panel,
                                    get_sp_1200_panel)
from ....qad.api import Features
from ....util import cusip_abbrev_to_full

from ....import api

    
class TestAPI(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(3)
        df_sp_500 = get_sp_500_panel(since=self.since, until=self.until)# default monthly
        df_sp_1200 = get_sp_1200_panel(since=self.since, until=self.until)
        self.dfs = {'sp_500': df_sp_500, 'sp_1200': df_sp_1200}
        self.missingness_tolerance = 0.1

    def test_methods(self):
        for panel_name in self.dfs.keys():
            panel = self.dfs[panel_name]
            methods = self.generate_test_methods(panel)
            columns = list(panel.columns)
            for method_name in methods:
                self.check_feature(method_name, panel_name, panel)
                panel = panel[columns]

    def test_exchange_rate(self):
        for panel in self.dfs.values():
            temp = panel.copy()
            temp = temp.features.exchange_rate(from_currency='USD',
                                               to_currency='GBP')
            assert 'exchange_rate' in temp.columns
            self.check_feature_missingness(temp, 'exchange_rate')

    def test_ibes_actuals(self):
        for panel in self.dfs.values():
            temp = panel.copy()
            metric_name = 'sales'
            temp = temp.features.ibes_actuals(metric_name, period_type=4)
            assert metric_name in temp.columns
            self.check_feature_missingness(temp, metric_name)
    
    @staticmethod
    def generate_test_methods(panel):
        broken = ['returns',  # requires analysis accessor
         'last_fiscal_end_dates',  # uses wrong date column on main dataframe
         'excess_return'  # too many NaN features ~ 13% allowed 10%
         ]
        takes_args = ['ibes_actuals', 'exchange_rate', 'add_feature']
        
        method_names = dir(panel.features)
        invalid_method_names = broken + takes_args
        valid_method_names = [name for name in method_names 
                              if not (name in invalid_method_names or name.startswith('_'))]
        return valid_method_names

    def check_feature(self, method_name, panel_name, panel):
        us_only_features = ['sp_500_market_weight', 'sp_1500_market_weight', 'tickers']
        if not (panel_name == 'sp_1200' and method_name in us_only_features):
            print("testing method: ", method_name)
            method = getattr(panel.features, method_name)
            if callable(method):
                try:
                    panel = method()
                except TypeError as e:
                    print(f'error: {e}')
                    print(f'{method}')
                # check that feature is added
                assert method_name in panel.columns

                # check completeness of feature in index
                expected_incomplete = ['merger_target_next_announce_date']
                if method_name not in expected_incomplete:
                    self.check_feature_missingness(panel, method_name)
                print("panel columns: ", panel.columns)

    def check_feature_missingness(self, panel, feature):
        assert (len(panel[panel.in_index == 1][panel[feature].isna()]) / len(
            panel[panel.in_index == 1])) < self.missingness_tolerance


if __name__ == '__main__':
    unittest.main()
