import unittest
import datetime
import pandas as pd

from ....compustat.compustat import Compustat
from ....util import NotInitializedException


class TestCompustat(unittest.TestCase):
    def setUp(self):
        self.since = (datetime.datetime.now() - pd.tseries.offsets.MonthEnd(3)).date()
        self.until = (datetime.datetime.now() - pd.tseries.offsets.MonthEnd()).date()
        self.compustat = Compustat()
        self.gvkeys = self.compustat.get_sp_500_index_membership(since=self.since, until=self.until)['gvkey'].dropna().unique()

    def test_init(self):
        if not self.compustat:
            raise NotInitializedException('Compustat not initialized!')

    def test_get_co_afnd_feature_since_until_by_gvkeys(self):
        # test feature from co_afnd1
        result = self.compustat.get_co_afnd_feature_since_until_by_gvkeys(self.since,
                                                                          self.until,
                                                                          self.gvkeys,
                                                                          table='co_afnd1',
                                                                          feature='capx',
                                                                          feature_name='capex')

        assert result.date.min() >= self.since
        assert result.date.max() <= self.until
        assert len(result.date.unique()) > 0
        assert len(result.date.unique()) <= (self.until - self.since).days
        assert 'capex' in result.columns


if __name__ == '__main__':
    unittest.main()
