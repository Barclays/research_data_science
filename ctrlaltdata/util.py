from stdnum import cusip as CusipClass
from stdnum.gb import sedol as SedolClass
from functools import wraps
import re
from money import XMoney
import numpy as np

from .config import enabled_modules


class NotEnabledException(Exception):
    pass


class NotInitializedException(Exception):
    pass


class NoIBESActualsFoundException(Exception):
    pass


def create_depends_on(enabled_modules):
    def depends_on(modules):
        def decorator_depends_on(func):
            @wraps(func)
            def wrapper_depends_on(*args, **kwargs):
                missing = []
                for module in modules:
                    if module not in enabled_modules:
                        missing.append(module)
                if len(missing) > 0:
                    raise NotEnabledException(f"Method depends on {modules}, but {missing} are not enabled in config.py!")
                return func(*args, **kwargs)
            return wrapper_depends_on
        return decorator_depends_on
    return depends_on

depends_on = create_depends_on(enabled_modules)


def cusip_abbrev_to_full(cusip8):
    """take an abbreviated cusip 8 digits long and add the checksum final digit"""
    return [i+CusipClass.calc_check_digit(i) for i in cusip8]


def sedol_abbrev_to_full(sedol6):
    """take an abbreviated sedol 6 digits long and add the checksum final digit"""
    return [i+SedolClass.calc_check_digit(i) for i in sedol6]


def clean_string(my_str):
    """
    Given a string, replaces all punctuations, special characters and whitespace with underscore.
    Best use case is to pass string to be used as column names, especially when passing to database.py
    Note that SQL alias string cannot start with a number, which the function takes care of.
    :param my_str: String to clean
    :return: Cleaned string
    """
    my_new_string = re.sub('[^a-zA-Z0-9 \n]', ' ', my_str)
    my_new_string = '_'.join(word.lower()
                            for word in my_new_string.split(' '))
    if my_new_string[0].isnumeric():
        my_new_string = 'a' + my_new_string

    return my_new_string

def convert_pence_to_pounds(df,feature_name,feature_currency_name):
    """
    For a dataframe with a feature and a column for its currency. convert pence value to pounds and the label too"""
    pence_indexes=df[feature_currency_name].isin(['GBp','IEp'])
    df.loc[pence_indexes,
                feature_name]=df.loc[pence_indexes,feature_name]/100
    df.loc[pence_indexes,feature_currency_name]=[i[:-1]+"P" for i in df.loc[pence_indexes,feature_currency_name]]
    return df

def convert_metric_to_currency_aware_column(df, metric_name, currency_column):
    """for a dataFrame df with a feature and its associated currency in another column.
    Create a XMoney/MoneyPandas currency aware metric feature columns
    :str metric_name: column name of the metric
    :str currency_column: columns name of the currency 
    """
    # only add currency for those with non null values
    # indexes_with_currency = ~df[currency_column].isna()
    # make sure  pence currencies converted out
    df = convert_pence_to_pounds(
        df, metric_name, currency_column)

    df.loc[df[currency_column].isna(), [metric_name]] = np.nan
    df.loc[df[currency_column].isna(), [currency_column]] = 'USD'
    non_null=(~df[metric_name].isna())
    df.loc[non_null,metric_name] = [XMoney(float(row[metric_name]), row[currency_column]) for _, row in df.loc[non_null].iterrows()]
    del df[currency_column]
    return df

def panel_method(method):
    @wraps(method)
    def method_wrapper(accessor, *args, **kwargs):
        value = method(accessor._obj, *args, **kwargs)
        accessor._obj = value
        return value
    return method_wrapper