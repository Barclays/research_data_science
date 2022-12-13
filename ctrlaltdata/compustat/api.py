import numpy as np
import pandas as pd
import logging
import datetime
from collections import defaultdict

from ..resource import ResourceManager


class Features():
    def revenue(self, quarter_end_date=False):
        """
        Add quarterly contemporaneous revenues for North American companies. 
        This is not a Point In Time feature, i.e. it forward looks for use in credit card data comparisons.
        sales() feature is Point in Time.
        Requires date and gvkey columns
        :param quarter_end_date: bool - if True returns quarter end dates
        :return: pandas dataframe
        input:
            import research_data_science.barclays.api as api
            df.features.revenue()
        output:
            security_key_name security_key       date  in_index security_key_abbrev   gvkey   revenue
                        cusip    25754A201 2020-03-31         1            25754A20  160211   873.102
                        cusip    855244109 2020-03-31         1            85524410  025434  5995.700
                        cusip    25754A201 2020-06-30         1            25754A20  160211   920.023
                        cusip    855244109 2020-06-30         1            85524410  025434  4222.100
        """
        logging.warning(
            """Adding revenue for the same period, which is not point in time! For point in time used sales(). 
            Revenue is currently only supported with quarterly frequency.""")
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        db = ResourceManager().compustat
        feature = db.get_quarterly_revenue(
            since=since, until=until, gvkeys=gvkeys)
        self._obj = self._obj.sort_values(['date'])
        feature = feature.sort_values(['quarter_end_date'])
        df = pd.merge_asof(self._obj, feature, left_on='date',
                           right_on='quarter_end_date', by='gvkey')
        if quarter_end_date == False:
            df = df.drop(['quarter_end_date'], axis=1)
        df = df.rename(columns={'revtq': 'revenue'})
        return df

    def gvkey_compustat(self):
        """
        Maps a list of 9 digits cusips to their respective gvkeys. For now only SP500 cusips have gvkey. It needs at least
        security_key_name and security_key
        :return: pandas dataframe
        input:
            import research_data_science.barclays.api as api
            df.features.gvkey()
        output
            security_key_name security_key       date  in_index security_key_abbrev   gvkey
                        cusip    25754A201 2020-03-31         1            25754A20  160211
                        cusip    25754A201 2020-06-30         1            25754A20  160211
                        cusip    25754A201 2020-09-30         1            25754A20  1602110
        """
        compustat = ResourceManager().compustat
        cusips = self._get_keys(abbreviated=False)['cusip']
        since, until = self._get_date_range()
        mapping = compustat.get_cusip_to_gvkey_table(since, until, cusips)

        mapping_dict = defaultdict(lambda: None)
        for k, v in zip(mapping.security_key, mapping.gvkey):
            mapping_dict[k] = v

        data = [(i, mapping_dict[i]) for i in cusips]
        feature = pd.DataFrame(data, columns=['security_key', 'gvkey'])
        return self._obj.merge(feature,
                               on='security_key')

    def cik(self):
        compustat = ResourceManager().compustat
        if 'gvkey' not in self._obj.columns:
            self._obj = self._obj.features.gvkey()
        gvkeys = self._obj.gvkey.dropna().unique()
        mapping = compustat.get_gvkey_to_cik_table(gvkeys)

        mapping_dict = defaultdict(lambda: None)
        for k, v in zip(mapping.gvkey, mapping.cik):
            mapping_dict[k] = v

        data = [(i, mapping_dict[i]) for i in gvkeys]
        feature = pd.DataFrame(data, columns=['gvkey', 'cik'])
        return self._obj.merge(feature, on='gvkey', how='left')

    def ipo_date(self):
        """
        Function to add *latest* IPO dates to a panel of securities

        Some cusips could have IPO dates in the future w.r.t. to the panel dates.

        This is because we do not yet handle PIT IPO dates for companies
        which relist their shares, say, after bankruptcy. You could proxy
        their original IPO dates with the earliest date their stock price is available.
        You can add stock price using the closing_price() feature.

        Returns
        -------

        """
        if 'gvkey' not in self._obj.columns:
            self._obj = self._obj.features.gvkey()
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_ipo_date(gvkeys)
        logging.warning(
            "Some cusips could have IPO dates in the future w.r.t. to the panel dates. .")
        return self._obj.merge(feature, on='gvkey', how='left')

    def sp_1500_total_return_index(self):
        '''
        This function adds sp_1500_total_return_index column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.sp_1500_total_return_index()
        Output:
            +-------------------+--------+------------+--------------+
            | security_key_name | gvkey  |    date    | idx_returns  |
            +-------------------+--------+------------+--------------+
            |       cusip       | 001045 | 2019-03-31 | 1021.4112678 |
            |       cusip       | 012886 | 2019-03-31 | 1021.411267  |
            +-------------------+--------+------------+--------------+
        '''
        compustat = ResourceManager().compustat
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        idx_returns = compustat.get_sp1500_returns(since, until)
        self._obj = self._asof_merge_feature(
            idx_returns, 'sp_1500_total_return_index', on='date', by=None)
        return self._obj

    def assets(self):
        '''
        This function adds assets column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.assets()
        Output:
            +-------------------+--------+------------+---------+
            | security_key_name | gvkey  |    date    |  assets |
            +-------------------+--------+------------+---------+
            |       cusip       | 001004 | 2019-03-31 |  1524.7 |
            |       cusip       | 023843 | 2019-03-31 | 139.749 |
            +-------------------+--------+------------+---------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_assets_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'assets', on='date', by='gvkey')

    def preferred_stock(self):
        '''
        This function adds preferred_stock column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.preferred_stock()
        Output:
            +-------------------+--------+------------+-----------------+
            | security_key_name | gvkey  |    date    | preferred_stock |
            +-------------------+--------+------------+-----------------+
            |       cusip       | 001004 | 2019-03-31 |       0.0       |
            |       cusip       | 023843 | 2019-03-31 |       0.0       |
            +-------------------+--------+------------+-----------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_preferred_stock_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'preferred_stock', on='date', by='gvkey')

    def ebitda_compustat(self):
        '''
        This function adds ebitda column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.ebitda()
        Output:
            +-------------------+--------+------------+--------+
            | security_key_name | gvkey  |    date    | ebitda |
            +-------------------+--------+------------+--------+
            |       cusip       | 001004 | 2019-03-31 | 126.5  |
            |       cusip       | 023843 | 2019-03-31 | 48.385 |
            +-------------------+--------+------------+--------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_ebitda_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'ebitda', on='date', by='gvkey')

    def capex(self):
        '''
        This function adds capex column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.capex()
        Output:
            +-------------------+--------+------------+-------+
            | security_key_name | gvkey  |    date    | capex |
            +-------------------+--------+------------+-------+
            |       cusip       | 001004 | 2019-03-31 |  22.0 |
            |       cusip       | 023843 | 2019-03-31 | 3.982 |
            +-------------------+--------+------------+-------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_capex_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'capex', on='date', by='gvkey')

    def net_income(self):
        '''
        This function adds net_income column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.net_income()
        Outcome:
            +-------------------+--------+------------+------------+
            | security_key_name | gvkey  |    date    | net_income |
            +-------------------+--------+------------+------------+
            |       cusip       | 001004 | 2019-03-31 |    15.6    |
            |       cusip       | 023843 | 2019-03-31 |   20.476   |
            +-------------------+--------+------------+------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_net_income_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'net_income', on='date', by='gvkey')

    def r_and_d_expenses(self):
        '''
        This function adds r and d expenses  column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.r_and_d_expenses()
        Outcome:
            +-------------------+--------+------------+------------------+
            | security_key_name | gvkey  |    date    | r_and_d_expenses |
            +-------------------+--------+------------+------------------+
            |       cusip       | 179621 | 2019-12-31 |   1021.4112678   |
            |       cusip       | 316056 | 2019-12-31 |   1021.411267    |
            +-------------------+--------+------------+------------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_r_and_d_expenses_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'r_and_d_expenses', on='date', by='gvkey')

    def filing_date_10q(self):
        '''
        This function adds filing_date_10q column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.filing_date_10q()
        Output:
            +-------------------+--------+------------+-----------------+
            | security_key_name | gvkey  |    date    | filing_date_10q |
            +-------------------+--------+------------+-----------------+
            |       cusip       | 001004 | 2019-03-31 |    2019-03-20   |
            |       cusip       | 023843 | 2019-12-31 |    2019-10-30   |
            +-------------------+--------+------------+-----------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_filing_date_10q_since_until_by_gvkeys(since, until, gvkeys).rename(
            columns={'filing_date': 'filing_date_10q'})
        return self._asof_merge_feature(feature, 'filing_date_10q', on='date', by='gvkey')

    def sales_compustat(self):
        '''
        This function adds sales data column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.sales()
        Output:
            +-------------------+--------+------------+---------+
            | security_key_name | gvkey  |    date    |  sales  |
            +-------------------+--------+------------+---------+
            |       cusip       | 001045 | 2019-03-31 | 44541.0 |
            |       cusip       | 012886 | 2019-03-31 | 30400.0 |
            +-------------------+--------+------------+---------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_sales_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'sales', on='date', by='gvkey')

    def long_term_debt(self):
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_quarterly_long_term_debt(since, until, gvkeys)
        return self._asof_merge_feature(feature, 'long_term_debt', on='date', by='gvkey')

    def gic_compustat(self, add_gic_desc=False):
        compustat = ResourceManager().compustat
        if 'gvkey' not in self._obj:
            self._obj = self._obj.features.gvkey(source='compustat')
        since, until = self._get_date_range(
            delta=datetime.timedelta(days=60))
        feature = compustat.get_gvkey_to_gic_since_until(
            self._get_gvkeys(), since, until)
        self._warn_if_overwriting_and_delete('gsector')
        self._obj = self._obj.sort_values('date')
        feature = feature.sort_values('GICSfrom')
        self._obj = pd.merge_asof(
            self._obj, feature, left_on='date', right_on='GICSfrom', by='gvkey')
        bad_end_dates = self._obj[((~self._obj['GICSthru'].isna()) & (
                self._obj['GICSthru'] <= self._obj.date))].index
        if not bad_end_dates.empty:
            self._obj.loc[bad_end_dates, 'gic'] = np.NaN

        self._obj['gic'] = self._obj['gic'].fillna(
            "-1").astype(str)

        if add_gic_desc:
            mapping_dict = compustat.get_gics_codes_dictionaries()
            self._obj['sector'] = self._obj['gic'].str[:2].map(
                mapping_dict['gsector'])
            self._obj['industry_group'] = self._obj['gic'].str[:4].map(
                mapping_dict['ggroup'])
            self._obj['industry'] = self._obj['gic'].str[:6].map(
                mapping_dict['gind'])
            self._obj['sub_industry'] = self._obj['gic'].map(
                mapping_dict['gsubind'])

        return self._obj.drop(columns=['GICSfrom', 'GICSthru'])

    def total_revenue(self):
        '''
        This function adds total revenue data column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.total_revenue()
        Output:
            +-------------------+--------+------------+---------------+
            | security_key_name | gvkey  |    date    | total_revenue |
            +-------------------+--------+------------+---------------+
            |       cusip       | 001045 | 2019-03-31 |    44541.0    |
            |       cusip       | 012886 | 2019-03-31 |    30400.0    |
            +-------------------+--------+------------+---------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_total_revenue_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'total_revenue', on='date', by='gvkey')

    def total_liabilities(self):
        '''
        This function adds total liabilities column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.total_liabilities()
        Output:
            +-------------------+--------+------------+-------------------+
            | security_key_name | gvkey  |    date    | total_liabilities |
            +-------------------+--------+------------+-------------------+
            |       cusip       | 001045 | 2019-03-31 |      60749.0      |
            |       cusip       | 012886 | 2019-03-31 |      32269.0      |
            +-------------------+--------+------------+-------------------+
        '''
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_total_liabilities_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'total_liabilities', on='date', by='gvkey')

    def sp_500_returns(self):
        if 'sp_500_total_return_index' not in self._obj.columns:
            self._obj = self._obj.features.sp_500_total_return_index()
        self._obj = self._obj.analysis.add_lagged_feature_on_date_by_key(
            'sp_500_total_return_index')
        self._obj['sp_500_returns'] = self._obj['sp_500_total_return_index'] / \
            self._obj['sp_500_total_return_index_lag(1)']
        return self._obj

    def sp_1500_returns(self):
        if 'sp_1500_total_return_index' not in self._obj.columns:
            self._obj = self._obj.features.sp_1500_total_return_index()
        self._obj = self._obj.analysis.add_lagged_feature_on_date_by_key(
            'sp_1500_total_return_index')
        self._obj['sp_1500_returns'] = self._obj['sp_1500_total_return_index'] / \
            self._obj['sp_1500_total_return_index_lag(1)']
        return self._obj

    def sp_500_total_return_index(self):
        '''
         This function adds sp_500_total_return_index column to the dataframe using 'security_key_name', 'date', and 'gvkey'
        :return: dataframe
        Example:
            df.features.sp_500_total_return_index()
        Output:
            +-------------------+--------+------------+-------------+
            | security_key_name | gvkey  |    date    | idx_returns |
            +-------------------+--------+------------+-------------+
            |       cusip       | 001045 | 2019-03-31 |  5664.46268 |
            |       cusip       | 012886 | 2019-03-31 |  5664.46268 |
            +-------------------+--------+------------+-------------+
        '''
        compustat = ResourceManager().compustat
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        idx_returns = compustat.get_sp500_returns(since, until)
        self._obj = self._asof_merge_feature(
            idx_returns, 'sp_500_total_return_index', on='date', by=None)
        return self._obj

    def sg_and_a(self):
        """
        Function to add selling, general and administrative expenses to the panel.
        The base feature in Compustat includes R&D expenses in its value, so we
        subtract that in the database layer before returning this feature.
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_sg_and_a_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'sg_and_a', on='date', by='gvkey')

    def operating_cash_flow(self):
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_operating_cash_flow_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'operating_cash_flow', on='date', by='gvkey')

    def free_cash_flow_compustat(self):
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_free_cash_flow_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'free_cash_flow', on='date', by='gvkey')

    def issuer_name_compustat(self):
        """

        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_issuer_name(gvkeys)
        return self._obj.merge(feature, on='gvkey', how='left')

    def common_shares_compustat(self):
        since, until = self._get_date_range(delta=datetime.timedelta(days=372))
        gvkeys = self._get_gvkeys()
        compustat = ResourceManager().compustat
        feature = compustat.get_common_shares_since_until_by_gvkeys(
            since, until, gvkeys)
        return self._asof_merge_feature(feature, 'common_shares', on='date', by='gvkey')
