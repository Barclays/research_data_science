import unittest
import pandas as pd
import datetime

from ...api import BaseFeaturesAccessor
from ...panel_constructors import get_sp_1200_panel
from ...util import NotInitializedException


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(3)
        df_sp_1200 = get_sp_1200_panel(since=self.since, until=self.until)
        self.obj = BaseFeaturesAccessor(df_sp_1200)

    def test_init(self):
        if not self.obj:
            raise NotInitializedException('BaseFeaturesAccessor not initialized!') 
        
        assert all([key in self.obj.supported_keys for key in ['cusip', 'sedol']])
        assert all([key in self.obj.unit_key for key in ['security_key', 'security_key_name']])
        assert all([key in self.obj.time_key for key in ['date']])
        
        temp = self.obj._obj.copy()
        temp['cusip'] = temp['security_key'].copy()
        self.assertRaises(KeyError, self.obj._validate,
                          temp.drop(columns=['security_key_name', 'security_key']))
    
    def test_get_date_range(self):
        orig_since = self.obj._obj.date.min()
        orig_until = self.obj._obj.date.max()

        # check with 0 delta
        since, until = self.obj._get_date_range(delta=datetime.timedelta(days=0))
        assert since == orig_since
        assert until == orig_until

        # check with positive delta
        days = 10
        since, until = self.obj._get_date_range(delta=datetime.timedelta(days=days))
        assert ((until - since).days - (orig_until - orig_since).days) == 2*days

    def test_get_keys(self):
        keys = ['cusip', 'sedol']

        def check_keys(key_dict, key_col):
            for key in keys:
                assert key in key_dict.keys()
                assert all([x in key_dict[key] for x in self.obj._obj.loc[self.obj._obj['security_key_name'] == key, 
                                                                          key_col].unique()]
                          )
 
        unabbreviated_keys = self.obj._get_keys()
        check_keys(unabbreviated_keys, 'security_key')

        self.obj._obj = self.obj._obj.drop(columns=['security_key_abbrev'])
        abbreviated_keys = self.obj._get_keys(abbreviated=True)
        assert 'security_key_abbrev' in self.obj._obj.columns
        assert (self.obj._obj.loc[self.obj._obj['security_key_name'] == "cusip", 'security_key_abbrev'].str.len() == 8).all()
        assert (self.obj._obj.loc[self.obj._obj['security_key_name'] == "sedol", 'security_key_abbrev'].str.len() == 6).all()
        check_keys(abbreviated_keys, 'security_key_abbrev')

        # panel without cusip/sedol
        self.obj._obj['security_key_name'] = self.obj._obj['security_key_name'].replace('cusip', 'key1')
        self.obj._obj['security_key_name'] = self.obj._obj['security_key_name'].replace('sedol', 'key2')
        alt_keys = self.obj._get_keys()
        for key in keys:
            assert key in alt_keys
            assert not alt_keys[key]

    def test_asof_merge_feature(self):
        feature_name = 'two_digit_key'
        shifted_panel = self.obj._obj[['security_key_name', 'security_key', 'date']].copy()
        shifted_panel[feature_name] = shifted_panel['security_key'].str[:2].copy()
        shifted_panel['date'] = shifted_panel['date'] - datetime.timedelta(days=1)
        shifted_panel['shifted_date'] = shifted_panel['date'].copy()

        self.obj._obj[feature_name] = None
        as_of_merged_panel = self.obj._asof_merge_feature(shifted_panel, feature_name)

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
        
        panel_method = self.obj.add_feature(cusip_prefix, as_name='get_cusip_prefix')
        temp = self.obj.get_cusip_prefix()
        assert 'security_key_prefix' in temp.columns


if __name__ == '__main__':
    unittest.main()
