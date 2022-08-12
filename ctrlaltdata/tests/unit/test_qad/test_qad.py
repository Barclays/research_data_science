import unittest
import datetime
import pandas as pd


from ....qad.qad import QAD
from ....util import cusip_abbrev_to_full, NotInitializedException


class TestQAD(unittest.TestCase):
    def setUp(self):
        self.since = (datetime.datetime.now() - pd.tseries.offsets.MonthEnd(3)).date()
        self.until = (datetime.datetime.now() - pd.tseries.offsets.MonthEnd()).date()
        self.qad = QAD()
        self.membership = self.qad.get_daily_sp_index_membership(since=self.since,
                                                                 until=self.until)
        self.cusips = cusip_abbrev_to_full(self.membership.security_key_abbrev.unique())

    def test_init(self):
        if not self.qad:
            raise NotInitializedException('QAD not initialized!')

    def test_get_daily_sp_index_membership(self):
        # check expected columns
        expected_columns = set(['Cusip', 'security_key_abbrev', 'date', 'index_weight',
                                'security_key_name', 'in_index_since', 'in_index_until',
                                'total_return_index'])
        membership = self.membership
        print(membership.columns)
        assert set(membership.columns)==expected_columns

        # sanity check expected dates
        assert membership.date.min() >= self.since
        assert membership.date.max() <= self.until
        assert len(membership.date.unique()) > 0
        assert len(membership.date.unique()) <= (self.until - self.since).days

        # sanity check securities
        assert len(membership[membership.date == membership.date.max()].Cusip.unique()) >= 490
        assert len(membership[membership.date == membership.date.max()].Cusip.unique()) <= 510

        # sanity check data value coverage
        assert len(membership[membership.index_weight.isna()]) == 0
        assert len(membership[membership.in_index_since.isna()]) == 0


if __name__ == '__main__':
    unittest.main()
