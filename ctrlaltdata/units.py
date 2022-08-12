import logging
import pandas as pd
import datetime
import numpy as np
from moneypandas import MoneyArray
from money import XMoney
from decimal import Decimal

from .resource import ResourceManager


@pd.api.extensions.register_series_accessor("units")
class UnitsAccessor(object):
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.unit = None


@pd.api.extensions.register_dataframe_accessor("units")
class UnitsAccessor(object):
    def __init__(self, pandas_obj):
        """Expects a dataframe with unique columns on ['date', 'currency'] """
        self._obj = pandas_obj

    def _get_date_range(self, delta=datetime.timedelta(days=0)):
        since = self._obj.date.min() - delta
        until = self._obj.date.max() + delta
        return since, until

    def _validate_exchange_rate(self, use_datastream_price_currency=True):
        if not all(a in self._obj.columns for a in ['currency']):
            if use_datastream_price_currency:
                message = "Must have 'currency' so using datastream pricing currency on same day"
                logging.warning(message)
                self._obj = self._obj.features.share_price_currency(
                    exact_day_match=True, rename='currency')
            else:
                message = "Must have 'currency' column "
                raise AttributeError(message)

    def _get_currency_codes(self):
        self._validate_exchange_rate()
        unique_from_currency = self._obj.from_currency.dropna().unique()
        unique_to_currency = self._obj.to_currency.dropna().unique()
        return unique_from_currency, unique_to_currency

    def convert_currency(self, metric, to_currency='USD', exact_day_match=True):
        """
        changes foreign currency to us dollars at point in time
        These are mid_rates not close prices.
        exact_day_match=False grabs close prices from day before
        exact_day_match=True gets same day mid prices
        :param metric: str - the value to do the conversion for
        :param to_currency: str - default 'USD'
        :param exact_day_match: bool - the rate to consider for the change
        :return: pandas dataframe
        input:
            df[['date','closing_price', 'currency']].units.convert_currency(metric='closing_price', to_currency='USD')
        output:
                  date  closing_price currency  closing_price_usd
            2019-03-31      38.490006      AUD          27.341373
            2019-03-31      25.270005      AUD          17.950546
            2019-03-31      25.919999      AUD          18.412269
            2019-03-31      12.310001      AUD           8.744408
        """

        def add_same_currency_rate(df, since, until, to_currency):
            """ Helper function: creates a table to set to rate=1 if from_currency is same as to_currency"""
            date = pd.to_datetime(pd.date_range(
                start=since, end=until).tolist())
            temp = pd.DataFrame(data=date, columns=['date'])
            temp['from_currency'] = to_currency
            temp['to_currency'] = to_currency
            temp['exchange_rate'] = 1.0
            return pd.concat([df, temp]).reset_index(drop=1)

        def add_pence_rates(feature_df):
            """Some databases have UK sterling share prices / per share measures in pence GBp not pounds GBP
            This function adds pence rates from the pound rates that exist
            """
            has_pounds = feature_df[feature_df.from_currency == 'GBP']
            has_pounds.loc[:, 'from_currency'] = 'GBp'
            has_pounds.loc[:, 'exchange_rate'] = has_pounds.loc[:,
                                                                'exchange_rate']*100
            return pd.concat([feature_df, has_pounds])

        from_currency = np.unique(np.array(
            [cur.currency for cur in self._obj[metric].values if not type(cur) is float]))
        self._validate_exchange_rate()

        since, until = self._get_date_range(delta=datetime.timedelta(days=7))

        # if all the from_currency and to_currency are the same don't calculate currency
        if (from_currency[0] == to_currency) and (len(from_currency) == 1):
            self._obj[metric] = self._obj[metric].astype(float) 
            return self._obj

        # if we have pence but not pounds in the from_currency, add pounds and convert later using add_pence_rates
        if "GBp" in from_currency and "GBP" not in from_currency:
            from_currency = np.append(from_currency, np.array(["GBP"]))

        qad = ResourceManager().qad
        feature = qad.get_exchange_rate_since_until_by_currency_codes(
            since, until, from_currency, [to_currency])

        if "GBp" in from_currency:
            feature = add_pence_rates(feature)

        feature = add_same_currency_rate(feature, since, until, to_currency)
        feature.sort_values(['date', 'from_currency'], inplace=True)
        self._obj = pd.merge_asof(self._obj.sort_values(['date', 'currency']),
                                  feature.sort_values(
                                      ['date', 'from_currency']),
                                  on=['date'],
                                  right_by='from_currency',
                                  left_by='currency',
                                  allow_exact_matches=exact_day_match)
    
        self._obj[metric] = self._obj[metric].astype(float) / self._obj['exchange_rate']
        return self._obj.drop(['from_currency',	'to_currency', 'exchange_rate'], axis=1)

    def convert_currency_aware_column(self, metric, to_currency='USD', exact_day_match=True):
        """
        changes foreign currency to us dollars at point in time, when fed in a currency aware column with XMoney
        These are mid_rates not close prices.
        exact_day_match=False grabs close prices from day before
        exact_day_match=True gets same day mid prices
        :param metric: str - the column to convert 
        :param to_currency: str - default 'USD'
        :param exact_day_match: bool - the rate to consider for the change
        :return: pandas dataframe

        """
        def try_extract_currency(value):
            if type(value) == XMoney:
                return value.currency
            elif value is None:
                return np.nan
            else:
                raise TypeError("Value needs to be an XMoney object to perform currency operations.")
        has_metric = (~self._obj[metric].isna())
        self._obj.loc[has_metric, 'currency'] = self._obj.loc[has_metric, metric].map(try_extract_currency)
        self._obj = self.convert_currency(metric,
                                          to_currency=to_currency,
                                          exact_day_match=exact_day_match)
        return self._obj.drop(columns=['currency'])

    @staticmethod
    def convert_pence_to_pounds(df, feature_name, feature_currency_name):
        """
        For a dataframe with a feature and a column for its currency. convert pence value to pounds and the label too"""
        pence_indexes = df[feature_currency_name].isin(['GBp', 'IEp'])
        df.loc[pence_indexes,
               feature_name] = df.loc[pence_indexes, feature_name]/100
        df.loc[pence_indexes, feature_currency_name] = [
            i[:-1]+"P" for i in df.loc[pence_indexes, feature_currency_name]]
        return df
