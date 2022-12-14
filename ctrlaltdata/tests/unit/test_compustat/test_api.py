import unittest
import datetime
import pandas as pd

from ....api import get_sp_500_panel, get_sp_1500_panel
from ....compustat.api import Features


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(4)
        df_sp_500 = get_sp_500_panel(since=self.since, until=self.until)
        df_sp_1500 = get_sp_1500_panel(since=self.since, until=self.until)
        self.dfs = {'sp_500': df_sp_500, 'sp_1500': df_sp_1500}
        self.missingness_tolerance = 0.1

    def test_methods(self):
        for panel_name in self.dfs.keys():
            panel = self.dfs[panel_name]
            methods = self.generate_test_methods()
            columns = list(panel.columns)
            for method_name in methods:
                self.check_feature(method_name, panel)
                panel = panel[columns]

    @staticmethod
    def generate_test_methods():
        feature_obj = Features()
        valid_method_names = [name for name in dir(feature_obj)
                              if not (name.startswith('_') or name.endswith('compustat'))]
        return valid_method_names

    def check_feature(self, method_name, panel):
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
            expected_incomplete = ['ipo_date']
            if method_name not in expected_incomplete:
                self.check_feature_missingness(panel, method_name)
            print("panel columns: ", panel.columns)

    def check_feature_missingness(self, panel, feature):
        if 'return' in feature:
            # 'returns' functions will fail missing value test because
            # they are not calculated for the first panel date
            panel = panel[panel['date'] > panel['date'].min()]
        assert (len(panel[panel.in_index == 1][panel[feature].isna()]) / len(
            panel[panel.in_index == 1])) < self.missingness_tolerance


if __name__ == '__main__':
    unittest.main()
