import unittest
import datetime
import pandas as pd

from ....api import get_security_panel
from ....fred.api import Features


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.until = pd.to_datetime(datetime.datetime.now())
        self.since = self.until - pd.tseries.offsets.MonthEnd(4)
        self.df = get_security_panel(since=self.since, until=self.until, frequency='BM',
                                     cusips=[str(i) for i in range(50)])

    def test_get_fred_feature(self):
        fred_feature = self.df.features._get_fred_feature('ICSA')
        assert fred_feature.shape[0] > 0
        assert (fred_feature['value'] > 0).all()

    def test_return_fred_feature(self):
        fred_feature = self.df.features._get_fred_feature('ICSA')
        fred_feature_pit = self.df.features._return_fred_feature(fred_feature)
        assert (fred_feature_pit.groupby('date')['period_date'].count() == 1).all()

    def test_search_fred(self):
        assert self.df.features.search_fred('doesnotexist') is None
        assert self.df.features.search_fred('claims', filter=('frequency', 'Monthly')).shape[0] > 0

    def test_initial_claims(self):
        initial_claims = self.df.features.initial_claims()
        assert initial_claims.shape[0] > 0
        assert (initial_claims['value'] > 0).all()

    def test_gdp(self):
        gdp = self.df.features.gdp()
        assert gdp.shape[0] > 0
        assert (gdp['value'] > 0).all()
