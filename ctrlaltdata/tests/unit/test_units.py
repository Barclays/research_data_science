import unittest
import numpy as np
import pandas as pd
import datetime
from money import XMoney

from ...panel_constructors import (get_sp_500_panel, get_sp_1200_panel)
from ...units import UnitsAccessor
from ...util import NotInitializedException


class TestUnits(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(3)
        df_sp_500 = get_sp_500_panel(since=self.since, until=self.until)
        df_sp_1200 = get_sp_1200_panel(since=self.since, until=self.until)
        self.dfs = {'sp_500': df_sp_500, 'sp_1200': df_sp_1200}
        self.units_obj = {key: UnitsAccessor(self.dfs[key]) for key in self.dfs.keys()}

    def test_init(self):
        for units_obj in self.units_obj.values():
            if not units_obj:
                raise NotInitializedException('UnitsAccessor not initialized!')

    def test_get_date_range(self):
        days = 100
        for units_obj in self.units_obj.values():
            orig_since = units_obj._obj.date.min()
            orig_until = units_obj._obj.date.max()
            new_since, new_until = units_obj._get_date_range(datetime.timedelta(days=days))
            assert ((new_until - new_since).days - (orig_until - orig_since).days) == 2*days

    def test_validate_exchange_rate(self):
        for units_obj in self.units_obj.values():
            self.assertRaises(AttributeError, units_obj._validate_exchange_rate, 
                              use_datastream_price_currency=False)
            units_obj._validate_exchange_rate(use_datastream_price_currency=True)
            assert 'currency' in units_obj._obj.columns

    def create_currency_column(self, currency_list=None, make_currency_aware=True):
        if not currency_list:
            currency_list = ['EUR', 'USD', 'GBP']
        for units_obj in self.units_obj.values():
            units_obj._obj['currency'] = np.random.choice(currency_list, size=(units_obj._obj.shape[0]))
            units_obj._obj['dummy_feature'] = np.random.random(size=(units_obj._obj.shape[0]))
            if make_currency_aware:
                units_obj._obj['dummy_feature'] = units_obj._obj.apply(lambda row: XMoney(row['dummy_feature'],
                                                                                          row['currency']),
                                                                       axis=1)

    def test_convert_currency_aware_column(self):
        self.create_currency_column()
        for units_obj in self.units_obj.values():
            currency_converted_panel = units_obj.convert_currency_aware_column(metric='dummy_feature',
                                                                               to_currency='USD',
                                                                               exact_day_match=True)
            assert 'currency' not in currency_converted_panel.columns
            assert currency_converted_panel.dtypes['dummy_feature'] == float
        
    def test_all_currency_usd(self):
        self.create_currency_column(currency_list=['USD'])
        for units_obj in self.units_obj.values():
            currency_converted_panel = units_obj.convert_currency_aware_column(metric='dummy_feature',
                                                                               to_currency='USD',
                                                                               exact_day_match=True)
            assert 'currency' not in currency_converted_panel.columns
            assert currency_converted_panel.dtypes['dummy_feature'] == float

    def test_convert_pence_to_pounds(self):
        self.create_currency_column(currency_list=['GBp', 'IEp'], make_currency_aware=False)
        for units_obj in self.units_obj.values():
            panel = units_obj._obj.copy()
            panel['pence_values'] = panel['dummy_feature'].copy()
            panel = units_obj.convert_pence_to_pounds(df=panel,
                                                      feature_name='dummy_feature',
                                                      feature_currency_name='currency')
            assert np.allclose(panel['pence_values'], panel['dummy_feature']*100)


if __name__ == '__main__':
    unittest.main()
