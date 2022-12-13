import importlib
import logging
import pandas as pd
import numpy as np
import datetime


from .util import panel_method, source_finder
from .config import enabled_modules
from . import analysis
from . import units
from .panel_constructors import (
    get_security_panel,
    get_sp_panel,
    get_sp_500_panel,
    get_sp_1500_panel,
    get_index_from_datastream,
    get_gic_panel,
    get_sp_1200_panel
)


class BaseFeaturesAccessor(object):
    def __init__(self, pandas_obj):
        """
        Expects a `pandas.DataFrame with unique columns on ['security_key_name','security_key','date'].
        """
        self._obj = pandas_obj
        self.supported_keys = ['cusip', 'sedol']
        self._validate(pandas_obj)
        self.unit_key = self._get_unit_key()
        if 'date' in pandas_obj.columns:
            self.time_key = ['date']
        else:
            self.time_key = []

    def _validate(self, obj):
        """
        This method validates that the dataframe has the required fields to be used as a panel. This minimally
        means a unit key and a time key.

        The standard unit key is the pair ('security_key_name', 'security_key'), and the standard time key is 'date'.

        This raises warnings when the panel fails to validate. There is some experimental support for non-standard unit
        keys, with the intent to broaden support over time.

        :param obj: A pandas.DataFrame to be validated for use as a panel.
        :return: Returns None.
        """
        keys = ['security_key', 'security_key_name']
        if (not any(a in obj.columns for a in keys)) & any(a in obj.columns for a in self.supported_keys):
            raise KeyError("Security keys in the panel need to be reshaped as "
                           "'security_key' and 'security_key_name' columns")
        if 'security_key_name' not in obj.columns:
            logging.warning('security_key_name is a required column.')
        for k in obj.security_key_name.unique():
            if k not in self.supported_keys:
                logging.warning("{} is not supported!".format(k))
        if 'date' not in obj.columns:
            message = "No 'date' in dataframe. Won't be able to asof merge."
            logging.warning(message)

    def _get_unit_key(self):
        """
        A helper function to get names of unit key columns from the panel. These are usually
        ['security_key', 'security_key_name'].

        :return: list of strings consisting of the names of the panel keys.
        """
        unit_key = ['security_key', 'security_key_name']
        if all([key for key in self._obj.columns if key in unit_key]):
            return unit_key
        else:
            return []

    def _get_date_range(self, delta=datetime.timedelta(days=0)):
        """
        This is a helper function to infer a date range from the panel, usually for the purpose of passing those
        along from the API layer to the data layer to be interpolated into a query or request for data.

        There is some tolerance set around the min and max panels dates using the timedelta `delta`. This is meant so
        queries can extend beyond the panel ranges when e.g. fields require lagging data, or when the last reported
        value for a field happens before the earliest panel date to avoid the problem of having missing data in the
        first time step.

        :param self: The `features` accessor.
        :param delta: an offset-like object compatible with `datetime.datetime`s. The returns values will be the minimum
        panel date  minus the `delta`, and the maximum panel date plus the `delta`. Defaults to
        `datetime.timedelta(days=0)`.
        :return since, until: Returns two `datetime.datetime`s or similar objects, depending on the panel's date type.
        These represent a date range over which the panel extends (plus `delta`s).
        """
        since = self._obj.date.min() - delta
        until = self._obj.date.max() + delta
        return since, until

    def _get_keys(self, abbreviated=False):
        """
        This is a helper used to get the set of unique security keys found in the panel. It returns a dictionary with a
        key for each key type (currently, 'cusip' and 'sedol'), and values comprised of a list of strings corresponding
        to the unique cusips and sedols, respectively.

        When abbreviated keys are required (as with some databases) the `abbreviated` flag can be set to `True` to
        return the abbreviated keys instead.

        :param self: The features accessor.
        :param abbreviated: Defaults to `False`. Whether or not to return abbreviated keys.
        """
        if not abbreviated:
            key_dict = self._obj.groupby('security_key_name').apply(
                lambda x: x.security_key.unique()).to_dict()
        else:
            if 'security_key_abbrev' not in self._obj.columns:
                self._add_security_key_abbrev()
            key_dict = self._obj.groupby('security_key_name').apply(
                lambda x: x.security_key_abbrev.unique()).to_dict()
        for key_name in ['cusip', 'sedol']:
            if key_name not in key_dict:
                key_dict[key_name] = []
        return key_dict

    def _get_gvkeys(self):
        if 'gvkey' not in self._obj.columns:
            self._obj = self._obj.features.gvkey()
        return self._obj.gvkey.dropna().unique()

    def _add_security_key_abbrev(self):
        """
        Cusips and sedols have check digits which aren't used in some databases. This is a helper method to add
        abbreviate versions of these keys to the panel when necessary.

        :param self: the features accessor.
        :return: Has no return value.
        """
        self._obj.loc[self._obj.security_key_name == "cusip", 'security_key_abbrev'] = [
            i[:8] for i in self._obj.loc[self._obj.security_key_name == "cusip", 'security_key'].copy()]
        self._obj.loc[self._obj.security_key_name == "sedol", 'security_key_abbrev'] = [
            i[:6] for i in self._obj.loc[self._obj.security_key_name == "sedol", 'security_key'].copy()]

    def _asof_merge_feature(self, feature, feature_name, on='date', by=['security_key', 'security_key_name'],
                            exact_match_allowed=True):
        """
        Very often, securities data comes in with a defined frequency, but which doesn't align to a global sequence of
        time points. A common example is from quarterly company filings, where different companies might have different
        reporting dates, but which are all quarterly.

        When analyzing quarterly panels data, we often need aligned dates, but company reporting dates fall in between
        our panel dates. We use the convention to use the last reported data in our panels when this happens. Pandas can
        be verbose when using the merge_asof feature, so we put the common boilerplate here given that it is used with
        most features.

        :param feature: A pandas.DataFrame containing, minimally, the unit and time keys for the panel, and the feature
        to be merged onto the panel in long format.
        :param feature_name: A string indicating the name of the feature to be merged. This could match a column on
        `feature`.
        :param on: a string indicating the name of the time key in the panel and feature dataframes.
        :param by: a list of strings indicating the unit multi-key in the panel and feature dataframes.
        :param exact_match_allowed: a boolean (defaults to True) indicating whether to use exact day matches, or to
        consider such matches as matches to the next panel date. This choice is nuanced, and often depends on the exact
        intraday time numbers are reported compared with the exact time the outcome of interest occurs.
        """
        self._warn_if_overwriting_and_delete(feature_name)
        self._obj = self._obj.sort_values(on)
        feature = feature.sort_values(on)
        self._obj = pd.merge_asof(
            self._obj, feature, on=on, by=by, allow_exact_matches=exact_match_allowed)
        return self._obj

    def _warn_if_overwriting_and_delete(self, feature_name):
        """
        Sometimes, it is necessary for a feature request to be run silently to compute a derived feature. This may
        happen, for example, when the derived feature must be certain about which currency a currency-bearing field was
        requested in. This can result in unexpected behavior when the column already exists, and the values in it are
        of a different currency than the one required.

        To alert the analyst to this behavior, a warning is logged before overwriting the column. It is the best practice
        to use this method any time a column might be overwritten, but we can't guarantee this convention is followed
        everywhere.

        :param self: the features accessor.
        :param feature_name: the name of the feature being overwritten.
        """
        if feature_name in self._obj.columns:
            logging.warning(
                "Overwriting existing column '{}'.".format(feature_name))
            del self._obj[feature_name]

    @staticmethod
    def _add_rank_col(df, col, asc=True, group=[]):
        """
        Given columns to group on, column to rank and the order, adds an integer column called '{col}_rank' to the
        DataFrame.

        :param df: `pandas.DataFrame` containing the column to be ranked.
        :param col: A string specifying the column to rank.
        :param asc: Boolean indicating whether to rank in ascending (True) or descending order.
        :param group: A list of strings indicating columns to specifying discrete groups to rank within (if any).
        :return: the original dataframe with an additional column, `f'{col}_rank'`.
        """
        # [TODO: move this to analysis accessor]

        df[f'{col}_rank'] = df.groupby(group)[col].rank(ascending=asc)
        df[f'{col}_rank'] = df[f'{col}_rank'].astype(int)
        return df

    def add_feature(self, func, as_name=None):
        """
        This is for binding an experimental feature method to the features accessor, and is useful when testing a new
        feature. The feature should take the panel itself as the first argument. It will need to be modified when
        added to the features accessor if that's the eventual goal to get the panel from the features accessor as
        self._obj.

        :param func: A function that is meant to be bound to the features accessor. It takes the panel as the first
        argument, and has arbitrary other arguments.
        :param as_name: The name of the method, in case you'd like to rename it when binding to the panel.
        :return: The bound method. This probably isn't useful, since the method is now available at
        `panel.features.<method_name>`.
        """
        func = panel_method(func)
        if as_name is None:
            as_name = func.__name__
        bound_method = func.__get__(self, self.__class__)
        setattr(self, as_name, bound_method)
        return bound_method

    @source_finder(priority_list=['qad', 'compustat'])
    def gvkey(self, *args, source=None, **kwargs):
        """Add the gvkey (compustat identifier) for the panel securities.
        If the panel only has cusips, will use compustat db.
        Otherwise will use QADs mapping
        :returns: panel object with extra column for compustat idenifier gvkey """
        self._warn_if_overwriting_and_delete('gvkey')
        gvkey_call = getattr(self._obj.features, f'gvkey_{source}')
        return gvkey_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def issuer_name(self, *args, source=None, **kwargs):
        """
        This function returns the issuer name
        :param source: string -default 'QAD'. 'compustat' does not workfor global securities
        However compustat is a quicker method
        """
        issuer_name_call = getattr(self._obj.features, f'issuer_name_{source}')
        return issuer_name_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def gic(self, *args, add_gic_desc=False, source=None, **kwargs):
        """
        This function returns the issuer name
        :param source: string -default 'QAD'. 'compustat' does not workfor global securities
        :param add_gic_desc: boolean -default False. Flag to add GIC descriptions
        """
        gic_call = getattr(self._obj.features, f'gic_{source}')
        return gic_call(*args, add_gic_desc=add_gic_desc, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def sales(self, *args, source=None, **kwargs):
        """
        Adds a `sales` column to the panel.
        If `source` is not provided, it uses the sources enabled in the configuration file
        in the order in which they are listed
        If `source` is provided, variable is returned from that source's method.
        Raises an error if an invalid source is provided, or if the source is not enabled
        in the config file.

        Some sources may take additional keyword args. The details of those can be referred to
        in the function definition inside the source module.

        :param source: str, default None. The name of source from which the variable is requested
        :returns: Pandas Dataframe. Panel with `sales` column added
        """
        sales_call = getattr(self._obj.features, f'sales_{source}')
        return sales_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def ebitda(self, *args, source=None, **kwargs):
        """
         Adds a `ebitda` column to the panel.
        If `source` is not provided, it uses the sources enabled in the configuration file
        in the order in which they are listed.
        If `source` is provided, variable is returned from that source's method.
        Raises an error if an invalid source is provided, or if the source is not enabled
        in the config file.

        Some sources may take additional keyword args. The details of those can be referred to
        in the function definition inside the source module.

        :param source: str, default None. The name of source from which the variable is requested
        :returns: Pandas Dataframe. Panel with `ebitda` column added
        """
        ebitda_call = getattr(self._obj.features, f'ebitda_{source}')
        return ebitda_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def free_cash_flow(self, *args, source=None, **kwargs):
        """
        Adds a `free_cash_flow` column to the panel.
        If `source` is not provided, it uses the sources enabled in the configuration file
        in the order in which they are listed.
        If `source` is provided, variable is returned from that source's method.
        Raises an error if an invalid source is provided, or if the source is not enabled
        in the config file.

        Some sources may take additional keyword args. The details of those can be referred to
        in the function definition inside the source module.

        :param source: str, default None. The name of source from which the variable is requested
        :returns: Pandas Dataframe. Panel with `free_cash_flow` column added
        """
        free_cash_flow_call = getattr(self._obj.features, f'free_cash_flow_{source}')
        return free_cash_flow_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def common_shares(self, *args, source=None, **kwargs):
        """
        Adds a `common_shares` column to the panel.
        If `source` is not provided, it uses the sources enabled in the configuration file
        in the order in which they are listed.
        If `source` is provided, variable is returned from that source's method.
        Raises an error if an invalid source is provided, or if the source is not enabled
        in the config file.

        :param source: str, default None. The name of source from which the variable is requested
        :returns: Pandas Dataframe. Panel with `common_shares` column added
        """
        common_shares_call = getattr(self._obj.features, f'common_shares_{source}')
        return common_shares_call(*args, **kwargs)

    @source_finder(priority_list=['qad', 'compustat'])
    def gic4_sales_market_share_in_index(self, *args, source=None, **kwargs):
        # [TODO: remove qad dependence from gic_code, then move to compustat module]
        if 'gic4' not in self._obj.columns:
            if 'gic' not in self._obj.columns:
                self._obj = self._obj.features.gic(*args, source=source, **kwargs)
            self._obj['gic4'] = self._obj.gic.apply(lambda x: x[:4])
        if 'sales' not in self._obj.columns:
            self._obj = self._obj.features.sales(*args, source=source, **kwargs)
        market_share = self._obj[self._obj.in_index == 1].groupby(['date', 'gic4']) \
                                                         .sum()[['sales']] \
                                                         .rename(columns={'sales': 'gic_sales'})
        temp = self._obj.merge(market_share, on=['date', 'gic4'])
        temp['gic4_sales_market_share_in_index'] = temp['sales'] / temp['gic_sales']
        temp = temp[self.unit_key + self.time_key +
                    ['gic4_sales_market_share_in_index']]
        return self._asof_merge_feature(temp, 'gic4_sales_market_share_in_index')

    @source_finder(priority_list=['qad', 'compustat'])
    def hhi_by_gic4(self, *args, source=None, **kwargs):
        """
        Adds Herfindahl–Hirschman Index (HHI) calculated per gic4 level industry.
        More information about HHI can be found here: https://www.justice.gov/atr/herfindahl-hirschman-index
        Returns
        -------
        Panel with feature hhi_by_gic4
        """
        if 'gic4_sales_market_share_in_index' not in self._obj.columns:
            self._obj = self._obj.features.gic4_sales_market_share_in_index(*args, source=source, **kwargs)

        df_hhi = self._obj.groupby(['gic4', 'date'])['gic4_sales_market_share_in_index']\
            .apply(lambda x: np.sum(np.square(x))).reset_index().rename(columns={'gic4_sales_market_share_in_index': 'hhi_by_gic4'})

        return self._asof_merge_feature(df_hhi, 'hhi_by_gic4', on='date', by=['gic4'])


def build_features_accessor(enabled_modules):
    if __package__:
        context = __package__
    else:
        context =  'research_data_science.research_data_science_public.ctrlaltdata'

    feature_classes = [importlib.import_module(f'{context}.{module}.api').Features for module in enabled_modules]
    feature_classes.append(BaseFeaturesAccessor)


    @pd.api.extensions.register_dataframe_accessor("features")
    class FeaturesAccessor(*feature_classes):
        def __init__(self, pandas_obj):
            BaseFeaturesAccessor.__init__(self, pandas_obj)

    return FeaturesAccessor

FeaturesAccessor = build_features_accessor(enabled_modules)


def load_panel_from_disk(path):
    """
    Reads a panel built by the panel tool from a csv, xls, xlsx, xlsm, xlsb, odf, ods or odt type file.
    It enforces correct data types for panel keys like security_key, security_key_abbrev, gvkey, and date.

    Without using this, typically dates are read as strings, and gvkeys are read as integers (dropping their
    meaningful leading zeros). Cusips may also be read as strings, depending on whether the panel's universe contains
    alphanumeric or just numeric cusips.

    :param path: A string. The path to the saved panel.
    :return: The panel loaded as a `pandas.DataFrame`.
    """
    excel_extensions = ['xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt']
    panel_key_dict = {'security_key': str, 'security_key_abbrev': str, 'gvkey': str}
    if path.endswith('.csv'):
        return pd.read_csv(path, dtype=panel_key_dict, parse_dates=['date'])
    elif any([path.endswith('.{}'.format(ext)) for ext in excel_extensions]):
        return pd.read_excel(path, dtype=panel_key_dict, parse_dates=['date'])
