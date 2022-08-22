import pandas as pd
import logging
import datetime
import numpy as np
from pandas.tseries.offsets import DateOffset


from ..resource import ResourceManager
from ..util import (convert_metric_to_currency_aware_column,
                    NoIBESActualsFoundException
                    )


class Features():
    def tickers(self):
        """
        Adds a list of tickers to the panel for `security_key_name` == "cusip"
        corresponding to the different North American exchanges they are listed on.
        TODO: Modify to return a unique ticker for a single exchange

        :returns: panel with a `tickers` column
        """
        if not self._obj[self._obj['security_key_name'] == 'sedol'].empty:
            logging.warning("This function only works for North American securities/cusips. "
                            "Will not attach ticker for global securities in the panel")
        cusips = self._get_keys(abbreviated=True)['cusip']
        qad = ResourceManager().qad
        feature = qad.get_ticker_by_cusip(cusips)
        feature = feature.rename(columns={'ticker': 'tickers'})
        feature = feature.groupby('cusip')['tickers'].apply(list).reset_index()
        self._obj = self._obj.merge(feature,
                                    left_on='security_key_abbrev',
                                    right_on='cusip',
                                    how='left').drop(columns=['cusip'])
        return self._obj

    def total_return_index(self, exact_day_match=True):
        """ 
        This returns a return index (closing values).
        
        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :returns: panel with a `total_return_index` column
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))

        keys = self._get_keys(abbreviated=True)

        qad = ResourceManager().qad
        feature = qad.get_return_index_since_until_by_cusip_sedol(
            since, until, **keys)
        return self._asof_merge_feature(feature,
                                        'total_return_index',
                                        on='date',
                                        by=['security_key_abbrev',
                                            'security_key_name'],
                                        exact_match_allowed=exact_day_match)

    def sp_500_market_weight(self):
        """
        Adds market weights for S&P 500 as a fraction.
        
        :returns: panel with a `sp_500_market_weight` column
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        keys = self._get_keys(abbreviated=True)

        qad = ResourceManager().qad
        feature = qad.get_sp_500_market_weights(since, until, keys['cusip'])
        feature['date'] = pd.to_datetime(feature['date'])

        return self._asof_merge_feature(feature, 'sp_500_market_weight', on='date',
                                        by=['security_key_abbrev', 'security_key_name'])

    def sp_1500_market_weight(self):
        """
        Adds market weights for S&P 1500 as a fraction.
        
        :returns: panel with a `sp_1500_market_weight` column
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        keys = self._get_keys(abbreviated=True)

        qad = ResourceManager().qad
        feature = qad.get_sp_1500_market_weights(since, until, keys['cusip'])
        feature['date'] = pd.to_datetime(feature['date'])

        return self._asof_merge_feature(feature, 'sp_1500_market_weight', on='date',
                                        by=['security_key_abbrev', 'security_key_name'])

    def seccode(self):
        """
        Adds `seccode` and `typ` to the panel where
        typ = 0 is for North American security
        typ = 6  is for a security from Rest of the World
        Securites are uniquely identified in QAD from these two keys
        and can be mapped to other databases via vw_securityMappingX
        or the vendor_code() feature.

        :returns: panel with a `typ` and `seccode` columns
        """
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.get_seccode_by_keys(**keys)
        return self._obj.merge(feature,
                               on=['security_key_abbrev', 'security_key_name'],
                               how='left')

    def exchange_rate(self, from_currency='USD', to_currency='USD', exact_day_match=True):
        """
        Adds exchange rate to the panel. These are mid_rates not close prices.

        :param from_currency: str, default "USD"
                        ISO 4217 currency code of the currency to convert from
        :param to_currency: str, default "USD"
                        ISO 4217 currency code of the currency to convert to
        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :returns: panel with `exchange_rate` column
        """
        columns = list(self._obj.columns) + ['exchange_rate']
        self._obj['from_currency'] = from_currency
        self._obj['to_currency'] = to_currency

        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        from_currency = [from_currency]
        to_currency = [to_currency]

        qad = ResourceManager().qad
        feature = qad.get_exchange_rate_since_until_by_currency_codes(
            since, until, from_currency, to_currency)
        result = self._asof_merge_feature(feature,
                                          'exchange_rate',
                                          on='date',
                                          by=['from_currency', 'to_currency'],
                                          exact_match_allowed=exact_day_match)
        return result[columns]

    def share_price_currency(self, exact_day_match=True, rename=False):
        """
        Adds share price currency from Datastream.

        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :param rename: bool, default False
                    If True, renames "share_price_currency" to "currency"
        :returns: panel with `share_price_currency` column
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        # can just choose adjustment type =0 (no adjustment), because only need currency column here
        feature = qad.get_closing_price_since_until_by_cusip_sedol(since, until,
                                                                   0, **keys)
        feature['date'] = pd.to_datetime(feature['date'])

        feature = feature[['security_key_abbrev', 'security_key_name', 'date',
                           'datastream_currency']]
        feature = feature.rename(
            columns={'datastream_currency': 'share_price_currency'})
        result = self._asof_merge_feature(feature,
                                          'share_price_currency',
                                          on='date',
                                          by=['security_key_abbrev',
                                              'security_key_name'],
                                          exact_match_allowed=exact_day_match)
        if rename:
            result = result.rename(
                columns={'share_price_currency': 'currency'})
        return result

    def closing_price(self, adj_type=0, exact_day_match=True, convert_currency=True, to_currency='USD'):
        """
        Adds closing price adjusted based on adjustment type. Also converts to specified currency if
        `convert_currency` is True.

        :param adj_type: int {0, 1, 2}, default 0
                    0: Unadjusted prices, 1: Adjusted for stock splits, 2: All adjustments
                    Use 2 for smooth prices returns, use 0 for actual price on day
        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :param convert_currency: bool, default True
                            - If True, convert added feature's currency to `to_currency`
                            - If False, keep as a currency aware object (XMoney)
        :param to_currency: str, default "USD"
                        ISO 4217 currency code of the currency to convert to
        :returns: panel with `closing_price` column
        """
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.get_closing_price_since_until_by_cusip_sedol(
            since, until, adj_type, **keys)
        feature['date'] = pd.to_datetime(feature['date'])

        feature = feature[['security_key_abbrev', 'security_key_name', 'date',
                           'datastream_closing_price', 'datastream_currency']]
        feature = feature.rename(columns={'datastream_closing_price': 'closing_price',
                                          'datastream_currency': 'currency'})
        # merge as of on minimal copy of panel, so don't add "currency" to panel
        df = self._obj.copy()[['security_key_abbrev', 'security_key_name', 'date']].features._asof_merge_feature(feature,
                                      'closing_price',
                                      by=['security_key_abbrev',
                                            'security_key_name'],
                                      exact_match_allowed=exact_day_match)
        df=convert_metric_to_currency_aware_column(df, 'closing_price', 'currency')
        if convert_currency:
            df = df.units.convert_currency_aware_column(
                metric='closing_price',  to_currency=to_currency)
        return self._obj.merge(df, on=['security_key_abbrev', 'security_key_name', 'date'], how='left')

    def closing_price_volatility(self, days=253, adj_type=2, normalise=False, normalise_method="average"):
        """
        Adds volatility of closing prices. To get a smooth time series unaffected by share splits and other corporate actions,
        we default to fully adjusted values, `adj_type`= 2.
        
        :param days: int, default 253 
                Number of trading days to calculate volatility over
        :param adj_type: int {0, 1, 2}, default 2
                    0: Unadjusted prices, 1: Adjusted for stock splits, 2: All adjustments
                    Use 2 for smooth prices returns, use 0 for actual price on day
        :param normalise: bool, default False
                    - If True, normalise volatility
                    - If False, do not normalise volatility
        :param normalise_method: str, default "average"
                        Method to normalise volatility.
                        Valid options are one of
                        - "average": average price over the period
                        - "start": first quoted price in period
                        - "end": last quoted price in period, i.e. panel date
        :returns: panel with `closing_price_volatility` column
        """
        since, until = self._get_date_range(
            delta=datetime.timedelta(days=days*2))
        keys = self._get_keys(abbreviated=True)

        qad = ResourceManager().qad
        feature = qad.get_closing_price_since_until_by_cusip_sedol(
            since, until, adj_type, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        if normalise and normalise_method not in ['average','start','end']:
            logging.warning(""" Only 'average','start','end' normalise_method are accepted, will return raw volatility values""")
        col_name="closing_price_volatility"
        feature2 = pd.DataFrame()
        for ix, group in feature.groupby('security_key_abbrev'):
            vol_value = (group.sort_values('date')
                            .datastream_closing_price
                            .bfill()   # rolling mean does not skip nans
                                        .rolling(days)
                                        .std())
            group = group.assign(closing_price_volatility=vol_value)
            if normalise and normalise_method == "average":
                
                mean_price=(group.sort_values('date')
                         .datastream_closing_price
                         .bfill()   # rolling mean does not skip nans
                                    .rolling(days)
                                    .mean())
                col_name="normalised_closing_price_volatility"
                group = group.assign(closing_price_mean=mean_price)
            
            feature2 = feature2.append(group)
        if normalise and normalise_method=="average":
            feature2.loc[:,col_name]=feature2.loc[:,'closing_price_volatility']/feature2.loc[:,'closing_price_mean']
        elif normalise and normalise_method=="start":
            col_name="normalised_closing_price_volatility"
            shifted=feature2.sort_values(['security_key_abbrev','date'])
            shifted.loc[:,'datastream_closing_price']=shifted['datastream_closing_price'].shift(days)
            shifted.rename(columns={'datastream_closing_price':'first_datastream_closing_price',
                       },inplace=True)
            shape_store=feature2.shape[0]
            feature2=feature2.merge(shifted[['first_datastream_closing_price','security_key_abbrev','security_key_name','date']],
                on=['security_key_abbrev','security_key_name','date'],how='left' )
            assert shape_store==feature2.shape[0]
            feature2.loc[:,col_name]=feature2.loc[:,'closing_price_volatility']/feature2.loc[:,'first_datastream_closing_price']
        elif normalise and normalise_method=="end":
            col_name="normalised_closing_price_volatility"
            feature2.loc[:,col_name]=feature2.loc[:,'closing_price_volatility']/feature2.loc[:,'datastream_closing_price']

        keep = ['security_key_abbrev', 'security_key_name',
                'date', col_name]
        return self._asof_merge_feature(feature2[keep],
                                        col_name,
                                        on='date',
                                        by=['security_key_abbrev', 'security_key_name'])

    def return_index_volatility(self, days=253, normalise=False, normalise_method="average_price"):
        """
        Adds volatility of the return index.

        :param days: int, default 253 
                Number of trading days to calculate volatility over
        :param normalise: bool, default False
                    - If True, normalise volatility
                    - If False, do not normalise volatility
        :param normalise_method: str, default "average"
                        Method to normalise volatility.
                        Valid options are one of
                        - "average": average price over the period
                        - "start": first quoted price in period
                        - "end": last quoted price in period, i.e. panel date
        :returns: panel with `return_index_volatility` column
        """
        since, until = self._get_date_range(
            delta=datetime.timedelta(days=days*2))
        keys = self._get_keys(abbreviated=True)

        qad = ResourceManager().qad
        feature = qad.get_return_index_since_until_by_cusip_sedol(
            since, until, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        if normalise and normalise_method not in ['average', 'start', 'end']:
            logging.warning("Only 'average','start','end' normalise_method are accepted, "
                            "will return raw volatility values")
        col_name="return_index_volatility"
        feature2 = pd.DataFrame()
        for ix, group in feature.groupby('security_key_abbrev'):
            vol_value = (group.sort_values('date')
                         .total_return_index
                         .bfill()  # rolling mean does not skip nans
                         .rolling(days)
                         .std())
            group = group.assign(return_index_volatility=vol_value)
            
            if normalise and normalise_method=="average":
                
                mean_ri=(group.sort_values('date')
                         .total_return_index
                         .bfill()   # rolling mean does not skip nans
                                    .rolling(days)
                                    .mean())
                col_name="normalised_return_index_volatility"
                group = group.assign(return_index_mean=mean_ri)
            feature2 = feature2.append(group)

        if normalise and normalise_method=="average":
            feature2.loc[:,col_name]=feature2.loc[:,'return_index_volatility']/feature2.loc[:,'return_index_mean']
        elif normalise and normalise_method=="start":
            col_name="normalised_return_index_volatility"
            shifted=feature2.sort_values(['security_key_abbrev','date'])
            shifted.loc[:,'total_return_index']=shifted['total_return_index'].shift(days)
            shifted.rename(columns={'total_return_index':'first_total_return_index',
                       },inplace=True)
            shape_store=feature2.shape[0]
            feature2=feature2.merge(shifted[['first_total_return_index','security_key_abbrev','security_key_name','date']],
                on=['security_key_abbrev','security_key_name','date'],how='left' )
            assert shape_store==feature2.shape[0]
            feature2.loc[:,col_name]=feature2.loc[:,'return_index_volatility']/feature2.loc[:,'first_total_return_index']
        elif normalise and normalise_method=="end":
            col_name="normalised_return_index_volatility"
            feature2.loc[:,col_name]=feature2.loc[:,'return_index_volatility']/feature2.loc[:,'total_return_index']
        keep = ['security_key_abbrev', 'security_key_name',
                'date', col_name]
        return self._asof_merge_feature(feature2[keep],
                                        col_name,
                                        on='date',
                                        by=['security_key_abbrev', 'security_key_name'])

    def consolidated_market_value(self, exact_day_match=True, convert_currency=True):
        """
        Adds consolidated market value for multiple listings i.e. the value of all shares
        available in all locations and classes.

        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :param convert_currency: bool, default True
                        - If True, convert added feature's currency to `to_currency`
                        - If False, keep as a currency aware object (XMoney)
        :returns: panel with `consolidated_market_value` column
        """
        logging.info(
            'Market Value is for all listings, if want one listing use market_cap()')
        if self._obj.date.min()<datetime.datetime(2000,1,1):
            logging.warning("""consolidated_market_value not filled before 2000 in datastream. 
                         Consider accumulating market caps for all relevant listings""")
        since, until = self._get_date_range(delta=datetime.timedelta(days=365))
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.get_market_value_since_until_by_cusip_sedol(
            since, until, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        # merge as of on minimal copy of panel, so can create currency aware columns on smaller object
        df = self._obj.copy()[['security_key_abbrev', 'security_key_name', 'date']].features._asof_merge_feature(feature,
                                      'consolidated_market_value',
                                      by=['security_key_abbrev',
                                            'security_key_name'],                                                                           
                                      exact_match_allowed=exact_day_match)
        df = convert_metric_to_currency_aware_column(df, 'consolidated_market_value', 'mkt_val_currency')
        if convert_currency:
            df = df.units.convert_currency_aware_column(metric='consolidated_market_value')
        else:
            logging.warning("Not converting to USD. Returning in native currency object")

        return self._obj.merge(df,on=['security_key_abbrev', 'security_key_name', 'date'], how='left')
    
    def market_cap(self, exact_day_match=True, convert_currency=True, free_float=False):
        """Returns market cap for one listing.
        :param exact_day_match: bool, if True gets same day close prices, if False grabs close prices from day before.
        :param convert_currency: bool, if True converts all values to USD (scalar), if False keep as currency aware.
        object (XMoney module).
        :param free_float: bool, if true grab the free float market cap rather than the overall.
        """
        logging.info(
            'Market Cap is for one listing, if want full value of all shares out, use consolidated_market_value')
        since, until = self._get_date_range(delta=datetime.timedelta(days=365))
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        if free_float:
            metric_name = "free_float_market_cap"
            feature = qad.get_free_float_market_cap_since_until_by_cusip_sedol(
                since, until, **keys)
        else:
            metric_name="market_cap"
            feature = qad.get_market_cap_since_until_by_cusip_sedol(
                since, until, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        df = self._obj.copy()[['security_key_abbrev', 'security_key_name', 'date']].features._asof_merge_feature(
            feature,
            metric_name,
            by=['security_key_abbrev', 'security_key_name'],
            exact_match_allowed=exact_day_match)
        df = convert_metric_to_currency_aware_column(
                df, metric_name, 'mkt_cap_currency')
        if convert_currency:
            df = df.units.convert_currency_aware_column(
                metric='market_cap')

        return self._obj.merge(df, on=['security_key_abbrev', 'security_key_name', 'date'],how='left')
    
    def consolidated_share_count(self, exact_day_match=True):
        """Returns consolidated share count for multiple listings.
        :param exact_day_match: bool, if True gets same day close prices, if False grabs close prices from day before.
        """
        logging.info(
            'consolidated_share_count is for all listings, if want one listing use common_shares()')
        if self._obj.date.min() < datetime.datetime(2000, 1, 1):
            logging.warning("""consolidated_share_count not filled before 2000 in datastream. 
                         Consider accumulating common_shares for all relevant listings""")
        since, until = self._get_date_range(delta=datetime.timedelta(days=365))
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.get_consolidated_share_count_since_until_by_cusip_sedol(
            since, until, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        
        return self._asof_merge_feature(feature,
                                        'consolidated_share_count',
                                        on='date',
                                        by=['security_key_abbrev', 'security_key_name'],
                                        exact_match_allowed=exact_day_match)

    def common_shares(self):
        """Returns shares for this listing."""
        logging.info(
                """common_shares is for once listing, if want all listings use consolidated_share_count(). 
                Note also that if share count does not change in the five years before the panel starts, 
                this metric will be empty.""")
        keys = self._get_keys(abbreviated=True)
        since, until = self._get_date_range(delta=datetime.timedelta(days=5*365))
        qad = ResourceManager().qad
        feature = qad.get_common_shares_one_listing(
            since, until, **keys)
        feature['date'] = pd.to_datetime(feature['date'])
        return self._asof_merge_feature(feature,
                                        'common_shares',
                                        on='date',
                                        by=['security_key_abbrev', 'security_key_name'])

    def ibes_key(self):
        """Adds estpermid column as ibes_key. This is a point in time mapping.
        For US/Canadian shared instruments, try to find the primary quote.
        If not, choose US listing.
        :return: Panel with ibes_key columns added.
        """
        qad = ResourceManager().qad
        keys = self._get_keys(abbreviated=True)
        ibes_key_candidates = qad.get_ibes_key(**keys)
            
        panel_all = self._obj.merge(ibes_key_candidates, on=["security_key_abbrev", "security_key_name"], how='left')
        # Filter that date must be in a valid forecast daterange
        # also re-sort the columns to rank preffered matches by:
        # 1 Is a primary quote
        # 2 Highest Rank (newest listing)
        # 3 Lowest exchange (NA only), chooses US
        panel_eff = panel_all[(panel_all.date >= panel_all.EffectiveDate) &
            (panel_all.date <= panel_all.ExpireDate)].sort_values([
            'security_key_abbrev', 'IsPrimary', 'Rank', 'Exchange'], ascending= [True, False, True, True])
        # then groupby security-date pairs and choose first row!
        grouped_eff = panel_eff.groupby(["security_key_abbrev", "security_key_name", "date"]).first()
        keep = ["security_key_abbrev", "security_key_name", "date", "ibes_key"]
        shape_before = self._obj.shape[0]
        self._obj = self._obj.merge(grouped_eff.reset_index()[keep],
                                    on=["security_key_abbrev", "security_key_name", "date"],
                                    how='left')
        # the panel should not have extra rows post the merge
        assert self._obj.shape[0] == shape_before
        return self._obj

    def ibes_actuals(self, metric_name, period_type=4, keep_period_end_date=False, keep_ibes_key=False,
                     convert_currency=True):
        """Attaches <period_type> actuals for metric <metric_name> for the last available period.
        :param metric_name: Specifies which metric to get forecasts for. use lookup_ibes_metrics in qad.py to get
        a mapping between metric codes and names.
        :param period_type: int specifying the type of period to get estimates for.
                            period_type	    Period
                            -----------     -------
                                1	        LongTerm
                                2	        Month
                                3	        Quarter
                                4	        Year
                                5	        HalfYear
        :param keep_period_end_date: bool, if True, add a column with the row specific period_end_date for the metric
        added to the panel.
        :param keep_ibes_key: bool.
        :param convert_currency: bool, if True converts all values to USD (scalar), if False keep as currency aware
        object (XMoney module).
        There are two relevant currency values. For the per share measures (EPS) and income statement measures (sales).
        For UK securities these values can be different (pence and pounds).
        :return: Panel with actuals column attached for the required metric.
        """
        keep = list(self._obj.columns) + [metric_name]
        qad = ResourceManager().qad
        since, until = self._get_date_range(delta=datetime.timedelta(days=365))
        if 'ibes_key' not in self._obj.columns:
            self._obj = self.ibes_key()
        ibes_keys = self._obj.ibes_key.dropna().unique()
        actuals = qad.get_ibes_actuals(since=since,
                                       until=until,
                                       metric_name=metric_name,
                                       ibes_keys=ibes_keys,
                                       period_type=period_type)

        if keep_period_end_date:
            keep += ['period_end_date_act']
        if keep_ibes_key:
            keep += ['ibes_key']
        if not actuals.empty:
            currency_dict = qad.ibes_currency_dictionary(actuals['DefCurrPermID'].unique())
            actuals.loc[:, 'ibes_currency_'+metric_name] = actuals.loc[:, 'DefCurrPermID'].map(currency_dict)
            actuals = convert_metric_to_currency_aware_column(actuals, metric_name, 'ibes_currency_'+metric_name)
            self._obj = self._asof_merge_feature(actuals,
                                                 metric_name,
                                                 on='date',
                                                 by=['ibes_key'])[keep]
            if convert_currency:
                self._obj = self._obj.units.convert_currency_aware_column(
                    metric=metric_name, exact_day_match=True)

        else:
            raise NoIBESActualsFoundException(
                "Try changing the period settings, e.g. to 3 if you're looking for quarterly filing data."
            )
        return self._obj

    def merger_target_next_announce_date(self):
        """Returns panel with merger_target_next_announce_date column added for North American securities. The
        merger_target_next_announce_date won't be added for global securities."""
        qad = ResourceManager().qad
        since, until = self._get_date_range(delta=datetime.timedelta(days=7))
        keys = self._get_keys(abbreviated=True)
        if len(keys['sedol']) > 0:
            logging.warning('This function only adds the date for North American securities, not for global ones.')
        feature = qad.get_merger_target_announcement_dates(
            since, until, **keys)
        feature = feature.sort_values('merger_target_announce_date')
        self._obj = self._obj.sort_values('date')
        result = pd.merge_asof(self._obj,
                               feature,
                               left_on='date',
                               right_on='merger_target_announce_date',
                               by=['security_key_name', 'security_key_abbrev'],
                               direction='forward')
        result = result.rename(columns={'merger_target_announce_date': 'merger_target_next_announce_date'})
        return result

    def returns(self, exact_day_match=True):
        """Adds total return prior panel date to current panel date for each security. To get percentage returns,
        subtract 1.0 from column.
        :param exact_day_match: bool, if True uses same day close prices, if False uses close prices from day before.
        """
        if 'total_return_index' not in self._obj.columns:
            self._obj = self._obj.features.total_return_index(exact_day_match=exact_day_match)
        self._obj = self._obj.analysis.add_lagged_feature_on_date_by_key('total_return_index')
        self._obj['returns'] = self._obj['total_return_index'] / self._obj['total_return_index_lag(1)']
        return self._obj

    def vendor_code(self, ventype=10, rename_column='vendor_code'):
        """For a given security, use the core QAD SecMapX tables to map from security_key + security_key_name to the ID
        code used in a specific product database within QAD.
        :param ventype: int, code to select a database (compustat/IBES/worldscope etc) as per docs and Table SecVenType.
        default 10, i.e. worldscope security level mapping (not company level), which has the correct period end dates.
        :param rename_column: str.
        :return: ven_code.
        Note that for some vendors a further transformation might be needed.
        I.e. VenType=4 (compustat) gives the SecIntCode (SecID for global) which can be transformed into gvkey and iid
        via the CSVSecurity (CSGSec) tables Default VenType =10, Worldscope because it has the correct period end dates.
        """
        qad = ResourceManager().qad
        keys = self._get_keys(abbreviated=True)
        feature = qad.get_vendor_code(ventype, **keys)
        feature.loc[:, 'ven_code'] = feature.loc[:, 'ven_code'].astype(str)
        if isinstance(rename_column, str):
            feature.rename(columns={'ven_code': rename_column}, inplace=True)
        elif rename_column:
            feature.rename(
                columns={'ven_code': qad.get_vendor_type_name(ventype)}, inplace=True)
        return pd.merge(self._obj, feature, on='security_key_abbrev', how='left')

    def last_fiscal_end_dates(self, period='Q', time_offset=80):
        """Returns the last fiscal end dates and reported dates as defined in the Worldscope database, that uses exact
        dates not approximations.
        :param period: str, default 'Q' for quarterly note these are periods as defined in the Worldscope documentation,
        NOT pandas periods.
        :param time_offset: int, default 80 - this is for tolerance in merge_asof.
            Examples include 'Q', 'A' = annual, 'B' = restated annual, 'S' = semi-annual, 'C' = current.
        """
        qad = ResourceManager().qad
        worldscope_company_mapping_col = 'Worldscope Company Mapping'
        if worldscope_company_mapping_col not in self._obj.columns:
            self._obj = self._obj.features.vendor_code(ventype=10, rename_column=True)

        codes_table = qad.get_last_fiscal_end_dates(self._obj, period)
        codes_table = codes_table.rename(
            columns={'code': worldscope_company_mapping_col})
        codes_table[worldscope_company_mapping_col] = codes_table[worldscope_company_mapping_col].astype(str)
        temp = self._obj[~self._obj[worldscope_company_mapping_col].isna()]
        temp = pd.merge_asof(temp.sort_values(by=['quarter_end_date', worldscope_company_mapping_col]),
                             codes_table.sort_values(by=['end_date', worldscope_company_mapping_col]),
                             left_on='quarter_end_date',
                             right_on='end_date',
                             by=worldscope_company_mapping_col,
                             direction='nearest',
                             tolerance=pd.Timedelta(days=time_offset))
        df = pd.concat(
            [temp, self._obj[self._obj[worldscope_company_mapping_col].isna()]])
        return df.drop([worldscope_company_mapping_col], axis=1)

    def total_debt(self, period='A', exact_match_allowed=True, convert_currency=True):
        """Function returns total debt on balance sheet in last published set of reports.
        :param period: str, period type (NOT pandas standard) to retrive debt from, either annual type ['A','B','G']
        or quarterly type ['E','Q','H','I','R','@'].
        :param exact_match_allowed: bool, Allow same day merges, if False joins with previous date.
        :param convert_currency: bool, if True converts all values to USD (scalar),  if False keep as currency aware
        object (XMoney module)."""
        qad = ResourceManager().qad
        if "worldscope_key" not in self._obj.columns:
            self._obj = qad.worldscope_key(self._obj)
        worldscope_keys = self._obj[~self._obj.worldscope_key.isna(
        )].worldscope_key.unique()
        feature = qad.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=3255, exact_match_allowed=exact_match_allowed)

        self._obj.loc[self._obj.worldscope_key.isna(), 'worldscope_key'] = -99

        self._obj = self._asof_merge_feature(feature,
                                             'total_debt',
                                             on='date',
                                             by=['worldscope_key'],
                                             exact_match_allowed=exact_match_allowed)
        self._obj.drop(columns=['worldscope_key'], inplace=True)

        metric_name = 'total_debt'
        if convert_currency:
            self._obj = self._obj.units.convert_currency_aware_column(
                metric=metric_name,  exact_day_match=exact_match_allowed)
        return self._obj

    def cash_and_equivalents(self, period='A', exact_match_allowed=True, convert_currency=True):
        """Function returns cash and cash equivalents on balance sheet in last published set of reports. Includes short
        term investments.
        :param period: str, period type (NOT pandas standard) to retrieve cash from, either annual type ['A','B','G']
        or quarterly type ['E','Q','H','I','R','@'].
        :param exact_match_allowed: bool, if True allows same day merges, if False joins with previous date.
        :param convert_currency: bool, if True converts all values to USD (scalar), if False keep as currency aware
        object (XMoney module).
        """
        qad = ResourceManager().qad
        if "worldscope_key" not in self._obj.columns:
            self._obj = qad.worldscope_key(self._obj)
        worldscope_keys = self._obj[~self._obj.worldscope_key.isna()].worldscope_key.unique()
        feature = qad.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=2005, exact_match_allowed=exact_match_allowed)
        feature.rename(columns={'cash___generic': 'cash_and_equivalents'}, inplace=True)
        self._obj.loc[self._obj.worldscope_key.isna(), 'worldscope_key'] = -99

        self._obj = self._asof_merge_feature(feature,
                                             'cash_and_equivalents',
                                             on='date',
                                             by=['worldscope_key'],
                                             exact_match_allowed=exact_match_allowed)
        self._obj.drop(columns=['worldscope_key'], inplace=True)
        metric_name = 'cash_and_equivalents'
        if convert_currency:
            self._obj = self._obj.units.convert_currency_aware_column(metric=metric_name, exact_day_match=True)
        return self._obj

    def net_cash(self, period='A', exact_match_allowed=True, convert_currency=True):
        """Adds the net cash (debt) variable to the panel, either from last Annual (A) or Quarterly (Q) results.
        :param period: str, period type (NOT pandas standard) to retrieve net cash from, either annual type
        ['A','B','G'] or quarterly type ['E','Q','H','I','R','@'].
        :param exact_match_allowed: bool, if True allows same day merges, if False joins with previous date.
        :param convert_currency: bool, if True converts all values to USD (scalar), if False keep as currency aware
        object (XMoney module).
        :returns: Panel with 'net_cash' column either all in USD or in the original currency.
        """

        # create fresh sub panel in case of different currency or period values for total_debt or cash already on panel
        df = self._obj.copy()[['security_key', 'security_key_name', 'date']]
        df = df.features.total_debt(
            period, exact_match_allowed, convert_currency)
        df = df.features.cash_and_equivalents(
            period=period, exact_match_allowed=exact_match_allowed,
            convert_currency=convert_currency)
        indexer = ~df.cash_and_equivalents.isna() & ~df.total_debt.isna()
        df.loc[indexer, 'net_cash'] = df.loc[indexer,
                                             'cash_and_equivalents'] - df.loc[indexer, 'total_debt']
        return self._obj.merge(df[['security_key', 'security_key_name', 'date', 'net_cash']],
                               on=['security_key', 'security_key_name', 'date'],
                               how='left')

    def enterprise_value(self, period='A', exact_match_allowed=True):
        """Defined as: EV = Consolidated Market Value - Net Cash + Preference Shares + Minority Shares.
        This method returns a scalar in $USD denomination and uses the consolidated market value of all listings.
        Cash includes short term investments and Period is not the same as Pandas periods.

        *NOTE*
        For 2007 and before, sometimes MI and PS are null. If we have a valid Market Value and net cash values,
        we assume this is an error and fill with 0 values to increase feature completeness.

        :param period: str, period type (NOT pandas standard) to retrieve net cash from, either annual type
        ['A','B','G'] or quarterly type ['E','Q','H','I','R','@'].
        :param exact_match_allowed: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).
        :returns: Panel with `enterprise_value` column in USD (scalar).
        """
        qad = ResourceManager().qad
        df = self._obj[['security_key', 'security_key_name', 'date']].copy()
        if "worldscope_key" not in df.columns:
            df = qad.worldscope_key(df)

        worldscope_keys = df[~df.worldscope_key.isna()].worldscope_key.unique()

        pref_stock_feature = qad.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=3451, exact_match_allowed=exact_match_allowed)

        minority_interest_feature = qad.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=3426, exact_match_allowed=exact_match_allowed)

        df.loc[df.worldscope_key.isna(), 'worldscope_key'] = "-99"

        df = df.features._asof_merge_feature(pref_stock_feature,
                                             'preferred_stock',
                                             on='date',
                                             by=['worldscope_key'],
                                             exact_match_allowed=exact_match_allowed)

        df = df.features._asof_merge_feature(minority_interest_feature,
                                             'minority_interest',
                                             on='date',
                                             by=['worldscope_key'],
                                             exact_match_allowed=exact_match_allowed)

        for metric_name in ['preferred_stock', 'minority_interest']:
            df = df.units.convert_currency_aware_column(
                metric=metric_name, exact_day_match=True)

        df = df.features.consolidated_market_value(exact_day_match=exact_match_allowed,
                                                   convert_currency=True)
        df = df.features.net_cash(period=period, exact_match_allowed=exact_match_allowed,
                                  convert_currency=True)

        mv_net_cash_not_null = ~(df['consolidated_market_value'].isna() & df['net_cash'].isna())
        pref_stock_minority_int_null = (df['preferred_stock'].isna() | df['minority_interest'].isna())

        if any(mv_net_cash_not_null & pref_stock_minority_int_null):
            logging.warning("Found NA values for Preferred Stock and/or Minority Interest."
                            "Filling them with 0s.")
            df['preferred_stock'] = np.where(mv_net_cash_not_null, 0, df['preferred_stock'])
            df['minority_interest'] = np.where(mv_net_cash_not_null, 0, df['minority_interest'])

        df['enterprise_value'] = df['consolidated_market_value'] - df['net_cash'] \
                                 + df['preferred_stock'] + df['minority_interest']

        df = df[['security_key', 'security_key_name', 'date', 'enterprise_value']]
        self._obj = self._asof_merge_feature(df,
                                             'enterprise_value',
                                             exact_match_allowed=exact_match_allowed)
        return self._obj

    def portfolio_return(self, weight_column='index_weight', column_name='portfolio_return'):
        """Adds portfolio-returns to the panel. The returns are computed according to the weights
        of the single assets which is provided by the `weight_column` parameter.

        :param weight_column: str, default 'index_weight'
        Specifies the column for the index weights.
        :param column_name: str, default 'portfolio_return'
        Specifies the name of the columns which contains the portfolio-returns in the final panel.
        :returns: Panel with `portfolio_return` in the `column_name` column.
        """
        columns = self._obj.columns.tolist()

        if 'returns' not in columns:
            self._obj = self._obj.features.returns()

        if weight_column not in columns:
            logging.warning(f"{weight_column} column not found. Adding a column with equal weights")
            self._obj[weight_column] = self._obj['in_index'].copy()
            self._obj[weight_column] = self._obj[weight_column]/self._obj.groupby(['date'])[weight_column].transform('sum')

        self._obj = self._obj.analysis.add_lagged_feature_on_date_by_key(weight_column, lag=-1)
        self._obj['weighted_asset_return'] = (self._obj[f'{weight_column}_lag(-1)'] * (self._obj['returns'] - 1.))
        period_return = self._obj.groupby('date')[['weighted_asset_return']].sum() + 1.
        period_return = period_return.loc[period_return.index > period_return.index.min()]  # since we dont have the first period.
        self._obj = self._obj.merge(period_return.rename(columns={'weighted_asset_return': column_name}),
                                    left_on='date',
                                    right_on='date').sort_values('date')
        return self._obj[columns + [column_name]]

    def excess_return(self, benchmark_weight_column='index_weight'):
        """Computes the excess-return using the portfolio-return measured against a benchmark.

        :param benchmark_weight_column: str, default 'index_weight'
        Specifies the weighting of the `benchmark_return` column.
        :returns: Panel with `excess_return` added to the columns.
        """
        logging.warning(f"Calculating excess return over benchmark defined by {benchmark_weight_column} weights.")
        columns = self._obj.columns.tolist()
        self._obj = self._obj.features.portfolio_return(weight_column=benchmark_weight_column,
                                                        column_name='benchmark_return')
        self._obj = self._obj.features.returns()
        self._obj['excess_return'] = self._obj['returns'] - self._obj['benchmark_return']
        return self._obj[columns + ['excess_return']]

    def returns_momentum(self, month_lag_start=12, month_lag_end=1, exact_day_match=True):
        """Calculate returns momentum between two lagged periods in months. The standard is between 12m and 1m lag.

        :param month_lag_start: int, default 12
            Number of months ago to start the momentum measure.
        :param month_lag_end: int, default 1
            Number of months ago to end the momentum measure
        :param exact_day_match: bool, default True
            - If True, allow matching with the same 'on' value
              (i.e. less-than-or-equal-to / greater-than-or-equal-to)

            - If False, don't match the same 'on' value
              (i.e., strictly less-than / strictly greater-than).`
        :returns: Panel with `returns_momentum` added to the columns.
        """
        # compile all the dates we need return values for
        # without assuming these are in the panel.
        # i.e can choose annual panel but 6m momentum
        date_series = self._obj['date'].drop_duplicates()
        dates_needed = [i+DateOffset(months=-month_lag_start)
                        for i in date_series]
        dates_needed = dates_needed + \
            [i+DateOffset(months=-month_lag_end) for i in date_series]
        dates_needed += [i for i in date_series]
        df = []
        for d in list(set(dates_needed)):
            for i, r in self._obj[['security_key', 'security_key_name']].drop_duplicates().iterrows():
                df.append([d, r['security_key_name'], r['security_key']])
        df = pd.DataFrame(
            df, columns=['date', 'security_key_name', 'security_key'])
        df.__init__(df)
        df = df.features.total_return_index(exact_day_match=exact_day_match)

        start_offset = pd.tseries.offsets.DateOffset(months=month_lag_start)
        df = df.analysis.add_offset_lagged_feature_on_date_by_key(
            'total_return_index', offset=start_offset)
        end_offset = pd.tseries.offsets.DateOffset(months=month_lag_end)
        df = df.analysis.add_offset_lagged_feature_on_date_by_key(
            'total_return_index', offset=end_offset)
        nam_s = f"total_return_index_lag({start_offset})"
        nam_e = f"total_return_index_lag({end_offset})"
        df = df[(~df[nam_s].isna()) & (~df[nam_e].isna())]
        df.loc[:, 'returns_momentum'] = df.loc[:, nam_e] / df.loc[:, nam_s]-1
        return self._obj.merge(df[['returns_momentum', 'date', 'security_key', 'security_key_name']],
                               on=['date', 'security_key', 'security_key_name'], how='left')

    def price_momentum(self, month_lag_start=12, month_lag_end=1, exact_day_match=True):
        """Calculate price momentum between two lagged periods in months. The standard is between 12m and 1m lag.
        
        :param month_lag_start: int, default 12
            Number of months ago to start the momentum measure.
        :param month_lag_end: int, default 1
            Number of months ago to end the momentum measure.
        :param exact_day_match: bool, default True
            When False grabs close prices from day before,
            when True gets same day close prices (could be in future from PIT view)
        :returns: Panel with `price_momentum` added to the columns.
        """
        # compile all the dates we need price values for
        # without assuming these are in the panel.
        # i.e can choose annual panel but 6m momentum
        date_series = self._obj['date'].drop_duplicates()
        dates_needed = [i+DateOffset(months=-month_lag_start)
                        for i in date_series]
        dates_needed += [i+DateOffset(months=-month_lag_end)
                         for i in date_series]
        dates_needed = dates_needed+[i for i in date_series]
        df = []
        for d in list(set(dates_needed)):
            for i, r in self._obj[['security_key', 'security_key_name']].drop_duplicates().iterrows():
                df.append([d, r['security_key_name'], r['security_key']])
        df = pd.DataFrame(
            df, columns=['date', 'security_key_name', 'security_key'])
        df.__init__(df)
        df = df.features.closing_price(
            adj_type=2, exact_day_match=exact_day_match)

        start_offset = pd.tseries.offsets.DateOffset(months=month_lag_start)
        df = df.analysis.add_offset_lagged_feature_on_date_by_key(
            'closing_price', offset=start_offset)
        end_offset = pd.tseries.offsets.DateOffset(months=month_lag_end)
        df = df.analysis.add_offset_lagged_feature_on_date_by_key(
            'closing_price', offset=end_offset)
        nam_s = f"closing_price_lag({start_offset})"
        nam_e = f"closing_price_lag({end_offset})"
        df = df[(~df[nam_s].isna()) & (~df[nam_e].isna())]
        df.loc[:, 'price_momentum'] = df.loc[:, nam_e] / df.loc[:, nam_s]-1
        return self._obj.merge(df[['price_momentum', 'date', 'security_key', 'security_key_name']],
                               on=['date', 'security_key', 'security_key_name'], how='left')

    def dividends_per_share(self, exact_match_allowed=True, convert_currency=True):
        """This function returns the last annual dividend per share payment.
        The total dividends per share declared during the calendar year for U.S. corporations.
        The total dividends per share declared during the fiscal year for non-U.S. corporations. 
        It includes extra dividends declared during the year.

        :param exact_match_allowed: bool, default True
            Allow same day merges, if False joins with previous date.
        :param convert_currency: bool, default True
            If True converts all values to USD (scalar),
            if False keep as currency aware object (XMoney module)
        :returns: Panel with `dividends_per_share` added to the columns.
        """
        qad = ResourceManager().qad
        if "worldscope_key" not in self._obj.columns:
            self._obj = qad.worldscope_key(self._obj)
        worldscope_keys = self._obj[~self._obj.worldscope_key.isna()].worldscope_key.unique()
        feature = qad.worldscope_add_last_actual(
            worldscope_keys, period='A', metric_code=5101, exact_match_allowed=exact_match_allowed)
        self._obj.loc[self._obj.worldscope_key.isna(), 'worldscope_key'] = -99

        self._obj = self._asof_merge_feature(feature,
                                      'dividends_per_share',
                                      on='date',
                                      by=['worldscope_key'],
                                      exact_match_allowed=exact_match_allowed)
        self._obj.drop(columns=['worldscope_key'], inplace=True)
        metric_name = 'dividends_per_share'
        if convert_currency:
            self._obj = self._obj.units.convert_currency_aware_column(
                metric=metric_name, exact_day_match=True)
        return self._obj

    def dividend_yield(self, exact_match_allowed=True):
        """This function returns the trailing dividend yield based on last close price.
        The total dividends per share declared during the calendar year for U.S. corporations are used.
        The total dividends per share declared during the fiscal year for non-U.S. corporations are used.
        It includes extra dividends declared during the year.

        :param exact_match_allowed: bool, default True
            Allow same day merges, if False joins with previous date
        :returns: Panel with `dividend_yield` added to the columns.
        """
        # create fresh sub panel in case of different currency or period values for total_debt or cash already on panel
        df = self._obj.copy()[['security_key', 'security_key_name', 'date']]
        df = df.features.dividends_per_share(
           exact_match_allowed, convert_currency=True)
        df = df.features.closing_price(adj_type=0, exact_day_match=exact_match_allowed, convert_currency=True, to_currency='USD')
            
        indexer = ~df.closing_price.isna() & ~df.dividends_per_share.isna()
        df.loc[indexer, 'dividend_yield'] = df.loc[indexer,
                                             'dividends_per_share']/df.loc[indexer, 'closing_price']
        return self._obj.merge(df[['security_key', 'security_key_name', 'date', 'dividend_yield']],
                               on=['security_key', 'security_key_name', 'date'],
                               how='left')

    def issuer_name(self):
        """Returns the issuer name from the core QAD tables

        :returns: Panel with `issuer_name` added to the columns.
        """
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.security_name_QAD_master_table( **keys)
        return self._obj.merge(feature,
                               on=['security_key_abbrev', 'security_key_name' ],
                               how='left')

    def issuer_ISIN(self):
        """Retrieves the issuer ISIN form QAD and adds it to the panel.

        :returns: Panel with `issuer_ISIN` added to the columns.
        """
        keys = self._get_keys(abbreviated=True)
        qad = ResourceManager().qad
        feature = qad.security_ISIN_QAD_master_table(**keys)
        return self._obj.merge(feature,
                               on=['security_key_abbrev', 'security_key_name' ],
                               how='left')

    def gross_profit_margin(self, period='A', exact_match_allowed=True):
        """ Function returns Gross Profit Margin. Also called Profitability Ratio.
        Defined as Gross Income / Net Sales or Revenues * 100.
        Note: The function returns the ratio after dividing by 100.
        :param period: str, default 'A'
                    period type (NOT pandas standard) to retrieve from,
                    either annual type ['A','B','G'] or quarterly type ['E,'Q','H','I','R','@']
        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        self._warn_if_overwriting_and_delete('gross_profit_margin')
        qad = ResourceManager().qad
        feature_panel = qad.get_worldscope_feature(self._obj, feature_name='gross_profit_margin', feature_code=8306,
                                                   period=period,
                                                   exact_match_allowed=exact_match_allowed,
                                                   convert_currency=False,
                                                   is_security_level=False)

        self._obj = self._obj.merge(feature_panel, on=self.unit_key + self.time_key, how='left')
        self._obj['gross_profit_margin'] = self._obj['gross_profit_margin'].astype(float) / 100
        return self._obj

    def sales(self, period='A', exact_match_allowed=True, convert_currency=True):
        """adds the worldscope "net sales or revenue variable" to the panel as 'sales' for brevity
        'NET SALES OR REVENUES represent gross sales and other operating revenue less discounts, returns and allowances.'
        :param period: str, period type (NOT pandas standard) to retrive net cash from,
            either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool. Allow same day merges, if False joins with previous date
        :param convert_currency: Bool, if True converts all values to USD (scalar), 
            if False keep as currency aware object (XMoney module)
        """
        db_column_name = 'net_sales_or_revenues'
        self._warn_if_overwriting_and_delete('sales')
        qad = ResourceManager().qad
        feature_panel = qad.get_worldscope_feature(self._obj, feature_name='sales', feature_code=1001, period=period,
                                                   exact_match_allowed=exact_match_allowed, convert_currency=convert_currency,
                                                   is_security_level=False, db_column_name=db_column_name)
        
        self._obj = self._obj.merge(feature_panel, on=self.unit_key + self.time_key, how='left')
        return self._obj

    def net_debt_by_ebitda_ratio(self, period='A', exact_match_allowed=True):
        """
        Net Debt(= -Net Cash) / EBITDA

        :param period: str, period type (NOT pandas standard) to retrieve net cash from,
            either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        df = self._obj[self.unit_key + ['date']].copy()
        df = df.features.net_cash(period=period, exact_match_allowed=exact_match_allowed, convert_currency=True)
        df = df.features.ebitda(period=period, exact_match_allowed=exact_match_allowed, convert_currency=True)
        
        df['net_debt_by_ebitda_ratio'] = -df['net_cash'] / df['ebitda']

        self._obj = self._asof_merge_feature(df[self.unit_key + ['date', 'net_debt_by_ebitda_ratio']],
                                             'net_debt_by_ebitda_ratio',
                                             exact_match_allowed=exact_match_allowed)
        return self._obj

    def book_to_price_ratio(self, exact_match_allowed=True):
        """
        BOOK VALUE PER SHARE represents the book value (proportioned common equity divided by outstanding
        shares) at the company's fiscal year end for non-U.S. corporations and at the end of the last calendar quarter
        for U.S. corporations.

        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        self._warn_if_overwriting_and_delete('book_to_price_ratio')
        qad = ResourceManager().qad
        feature_panel = qad.get_worldscope_feature(self._obj, feature_name='book_value_per_share', feature_code=5476,
                                                   period='A', exact_match_allowed=exact_match_allowed,
                                                   convert_currency=True, is_security_level=True)
        
        feature_panel = feature_panel.features.closing_price(adj_type=0, exact_day_match=exact_match_allowed,
                                                             convert_currency=True, to_currency='USD')

        feature_panel['book_to_price_ratio'] = feature_panel['book_value_per_share']/feature_panel['closing_price']

        self._obj = self._obj.merge(feature_panel[self.unit_key + ['date', 'book_to_price_ratio']],
                                    on=self.unit_key + ['date'],
                                    how='left')
        return self._obj

    def ebitda_by_enterprise_value_ratio(self, exact_match_allowed=True):
        """
        EBITDA/EV

        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        df = self._obj[self.unit_key + ['date']].copy()
        df = df.features.ebitda(period='A', exact_match_allowed=exact_match_allowed, convert_currency=True)
        df = df.features.enterprise_value(period='A', exact_match_allowed=exact_match_allowed)

        df['ebitda_by_enterprise_value_ratio'] = df['ebitda'] / df['enterprise_value']

        self._obj = self._asof_merge_feature(df[self.unit_key + ['date', 'ebitda_by_enterprise_value_ratio']],
                                             'ebitda_by_enterprise_value_ratio',
                                             exact_match_allowed=exact_match_allowed)
        return self._obj

    def net_debt_by_market_value_ratio(self, exact_match_allowed=True):
        """
        Net Debt (= negative of Net Cash) / Market Value. This ratio uses trailing annual figures.



        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        df = self._obj[self.unit_key + ['date']].copy()
        df = df.features.net_cash(period='A', exact_match_allowed=exact_match_allowed, convert_currency=True)
        df = df.features.consolidated_market_value(exact_day_match=exact_match_allowed, convert_currency=True)

        df['net_debt_by_market_value_ratio'] = -df['net_cash'] / df['consolidated_market_value']

        self._obj = self._asof_merge_feature(df[self.unit_key + ['date', 'net_debt_by_market_value_ratio']],
                                             'net_debt_by_market_value_ratio',
                                             exact_match_allowed=exact_match_allowed)
        return self._obj

    def ebitda(self, period='A', exact_match_allowed=True, convert_currency=True):
        """
        Function returns EBITDA.
        EARNINGS BEFORE INTEREST, TAXES, DEPRECIATION & AMORTIZATION (EBITDA) represent the earnings
        of a company before interest expense, income taxes and depreciation. It is calculated by taking the pre-tax
        income and adding back interest expense on debt and depreciation, depletion and amortization and
        subtracting interest capitalized.
        
       :param period: str, default 'A'
                    period type (NOT pandas standard) to retrive from,
                    either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        :param convert_currency: bool, default True
                            - If True, convert added feature's currency to `to_currency`
                            - If False, keep as a currency aware object (XMoney)
        :returns:
        """
        db_column_name = 'ebit___depreciation'
        self._warn_if_overwriting_and_delete('ebitda')
        qad = ResourceManager().qad

        feature_panel = qad.get_worldscope_feature(self._obj, feature_name='ebitda', feature_code=18198, period=period,
                                                   exact_match_allowed=exact_match_allowed, convert_currency=convert_currency,
                                                   is_security_level=False, db_column_name=db_column_name)

        self._obj = self._obj.merge(feature_panel, on=self.unit_key + self.time_key, how='left')
        return self._obj

    def earnings_per_share(self, period='A', exact_match_allowed=True, convert_currency=True, diluted_earnings=True):
        """adds the worldscope "Earnings Per Share" to the panel as `earnings_per_share`

        EARNINGS PER SHARE represents the earnings for the 12 months ended the last calendar quarter of the year
        for U.S. corporations and the fiscal year for non-U.S. corporations It represents the fully diluted earnings per
        share (field 05290) for US companies and basic earnings per share (field 05210) for other companies.

        :param period: str, period type (NOT pandas standard) to retrive net cash from,
            either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool. Allow same day merges, if False joins with previous date
        :param convert_currency: Bool, if True converts all values to USD (scalar),
            if False keep as currency aware object (XMoney module)
        """
        self._warn_if_overwriting_and_delete('earnings_per_share')
        qad = ResourceManager().qad

        if diluted_earnings:
            feature_code = 5290
            db_column_name = 'earnings_per_share___fully_diluted_shares___year'
        else:
            feature_code = 5210
            db_column_name = 'earnings_per_share___basic___year'

        feature_panel = qad.get_worldscope_feature(self._obj, feature_name='earnings_per_share',
                                                   feature_code=feature_code,
                                                   period=period,
                                                   exact_match_allowed=exact_match_allowed,
                                                   convert_currency=convert_currency,
                                                   is_security_level=False, db_column_name=db_column_name)

        self._obj = self._obj.merge(feature_panel, on=self.unit_key + self.time_key, how='left')
        return self._obj

    def ebitda_margin(self, period='A', exact_match_allowed=True):
        """
        EBITDA margin = EBITDA/Sales

        :param period: str, period type (NOT pandas standard) to retrieve net cash from,
            either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        """
        panel_key = self.unit_key + self.time_key
        df = self._obj[panel_key].copy()
        df = df.features.ebitda(period=period, exact_match_allowed=exact_match_allowed, convert_currency=True)
        df = df.features.sales(period=period, exact_match_allowed=exact_match_allowed, convert_currency=True)

        df['ebitda_margin'] = df['ebitda'] / df['sales']

        self._obj = self._obj.merge(df[panel_key + ['ebitda_margin']],
                                    on=panel_key, how='left')
        return self._obj
