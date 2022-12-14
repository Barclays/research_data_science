import unittest
import pandas as pd
import datetime

from ...api import BaseFeaturesAccessor
from ...panel_constructors import get_sp_500_panel, get_sp_1200_panel
from ...util import NotInitializedException
from ...config import enabled_modules


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.objs = {}
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(3)
        if 'qad' in enabled_modules:
            df_sp_1200 = get_sp_1200_panel(since=self.since, until=self.until)
            self.objs['qad'] = BaseFeaturesAccessor(df_sp_1200)
        if 'compustat' in enabled_modules:
            df_sp_500 = get_sp_500_panel(since=self.since, until=self.until, source='compustat')
            self.objs['compustat'] = BaseFeaturesAccessor(df_sp_500)
        self.missingness_tolerance = 0.1

    def test_init(self):
        for panel in self.objs.values():
            if not panel:
                raise NotInitializedException('BaseFeaturesAccessor not initialized!')

            assert all([key in panel.supported_keys for key in ['cusip', 'sedol']])
            assert all([key in panel.unit_key for key in ['security_key', 'security_key_name']])
            assert all([key in panel.time_key for key in ['date']])

            temp = panel._obj.copy()
            temp['cusip'] = temp['security_key'].copy()
            self.assertRaises(KeyError, panel._validate,
                              temp.drop(columns=['security_key_name', 'security_key']))
    
    def test_get_date_range(self):
        for panel in self.objs.values():
            orig_since = panel._obj.date.min()
            orig_until = panel._obj.date.max()

            # check with 0 delta
            since, until = panel._get_date_range(delta=datetime.timedelta(days=0))
            assert since == orig_since
            assert until == orig_until

            # check with positive delta
            days = 10
            since, until = panel._get_date_range(delta=datetime.timedelta(days=days))
            assert ((until - since).days - (orig_until - orig_since).days) == 2*days

    def test_get_keys(self):
        keys = ['cusip', 'sedol']

        def check_keys(key_dict, key_col, panel):
            for key in keys:
                assert key in key_dict.keys()
                assert all([x in key_dict[key] for x in panel._obj.loc[panel._obj['security_key_name'] == key,
                                                                          key_col].unique()]
                          )
        for panel in self.objs.values():
            unabbreviated_keys = panel._get_keys()
            check_keys(unabbreviated_keys, 'security_key', panel)

            panel._obj = panel._obj.drop(columns=['security_key_abbrev'])
            abbreviated_keys = panel._get_keys(abbreviated=True)
            assert 'security_key_abbrev' in panel._obj.columns
            assert (panel._obj.loc[panel._obj['security_key_name'] == "cusip", 'security_key_abbrev'].str.len() == 8).all()
            assert (panel._obj.loc[panel._obj['security_key_name'] == "sedol", 'security_key_abbrev'].str.len() == 6).all()
            check_keys(abbreviated_keys, 'security_key_abbrev', panel)

            # panel without cusip/sedol
            panel._obj['security_key_name'] = panel._obj['security_key_name'].replace('cusip', 'key1')
            panel._obj['security_key_name'] = panel._obj['security_key_name'].replace('sedol', 'key2')
            alt_keys = panel._get_keys()
            for key in keys:
                assert key in alt_keys
                assert not alt_keys[key]

    def test_asof_merge_feature(self):
        for panel in self.objs.values():
            feature_name = 'two_digit_key'
            shifted_panel = panel._obj[['security_key_name', 'security_key', 'date']].copy()
            shifted_panel[feature_name] = shifted_panel['security_key'].str[:2].copy()
            shifted_panel['date'] = shifted_panel['date'] - datetime.timedelta(days=1)
            shifted_panel['shifted_date'] = shifted_panel['date'].copy()

            panel._obj[feature_name] = None
            as_of_merged_panel = panel._asof_merge_feature(shifted_panel, feature_name)

            # assert merged column in df is different from original column with same name
            assert not as_of_merged_panel[feature_name].isna().all()
            # assert the method only joins with direction='backward'
            assert (as_of_merged_panel['date'] >= as_of_merged_panel['shifted_date']).all()
            # assert correct merging on keys
            assert (as_of_merged_panel[feature_name] == as_of_merged_panel['security_key'].str[:2]).all()

    def test_add_method(self):
        def cusip_prefix(df):
            df['security_key_prefix'] = df['security_key'].str[:4]
            return df
        for panel in self.objs.values():
            panel_method = panel.add_feature(cusip_prefix, as_name='get_cusip_prefix')
            temp = panel.get_cusip_prefix()
            assert 'security_key_prefix' in temp.columns

    def test_methods(self):
        for source in self.objs.keys():
            panel = self.objs[source]._obj
            methods = self.generate_test_methods()
            columns = list(panel.columns)
            for method_name in methods:
                self.check_feature(method_name, panel, source)
                panel = panel[columns]

    def generate_test_methods(self):
        invalid_objects = ['supported_keys', 'time_key', 'unit_key', 'add_feature', 'gvkey']
        valid_method_names = [name for name in dir(list(self.objs.values())[0])
                              if not (name in invalid_objects or name.startswith('_'))]
        return valid_method_names

    def check_feature(self, method_name, panel, source):
        print("testing method: ", method_name)
        method = getattr(panel.features, method_name)
        if callable(method):
            try:
                panel = method(source=source)
            except TypeError as e:
                print(f'error: {e}')
                print(f'{method}')
            # check that feature is added
            assert method_name in panel.columns

            # check completeness of feature in index
            expected_incomplete = []
            if method_name not in expected_incomplete:
                self.check_feature_missingness(panel, method_name)
            print("panel columns: ", panel.columns)

    def check_feature_missingness(self, panel, feature):
        assert (len(panel[panel.in_index == 1][panel[feature].isna()]) / len(
            panel[panel.in_index == 1])) < self.missingness_tolerance


if __name__ == '__main__':
    unittest.main()
