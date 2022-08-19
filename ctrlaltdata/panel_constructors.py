from collections import defaultdict
import pandas as pd
import itertools
import numpy as np
import logging

from .util import (cusip_abbrev_to_full,
                   sedol_abbrev_to_full)
from .resource import ResourceManager


def get_security_panel(since, until, cusips=[], sedols=[], frequency='BM'):
    dates = pd.date_range(since, until, freq=frequency)
    dfs = []
    if cusips:
        df = pd.DataFrame([{'security_key_name': 'cusip',
                            'security_key': cusip,
                            'date': d} for cusip, d in itertools.product(cusips, dates)])
        dfs.append(df)
    if sedols:
        df = pd.DataFrame([{'security_key_name': 'sedol',
                            'security_key': sedol,
                            'date': d} for sedol, d in itertools.product(sedols, dates)])
        dfs.append(df)
    if dfs:
        df = pd.concat(dfs)
        df['in_index'] = 1
        df = df.reset_index(drop=True)
        return df
    raise ValueError("You must pass cusips or sedols.")


def get_sp_panel(since, until, frequency='BM', index_name='S&P 500 INDEX'):
    """
    Get a panel for an index from QAD's S&P composition tables.

    Some canonical options for index_name are
    'S&P 500 INDEX', 'S&P 1500 INDEX - SUPER COMP'
    """
    qad = ResourceManager().qad
    membership = qad.get_daily_sp_index_membership(
        since, until, index_name=index_name)
    membership['date'] = pd.to_datetime(membership['date'])
    membership['in_index'] = 1 * (membership['index_weight'] > 0)

    constituents = membership[['security_key_abbrev', 'security_key_name']].drop_duplicates(
    ).dropna(subset=['security_key_abbrev'])

    dates = pd.date_range(start=since, end=until, freq=frequency)
    panel = []
    for date in dates:
        temp = constituents.copy()
        temp['date'] = date
        panel.append(temp)
    panel = pd.concat(panel)
    panel['security_key'] = cusip_abbrev_to_full(panel['security_key_abbrev'])

    dates = membership.date.unique()
    membership_panel = []
    for date in dates:
        temp = constituents.copy()
        temp['date'] = date
        membership_panel.append(temp)
    membership_panel = pd.concat(membership_panel)
    membership_panel['security_key'] = cusip_abbrev_to_full(
        membership_panel['security_key_abbrev'])

    membership_panel = membership_panel.sort_values('date')
    membership = membership.sort_values('date')
    membership_panel = pd.merge(membership_panel,
                                membership[['security_key_name', 'security_key_abbrev', 'date', 'in_index',
                                            'total_return_index', 'index_weight']],
                                on=['date', 'security_key_name',
                                    'security_key_abbrev'],
                                how='left'
                                )

    # on days with index data, fill NAs with 0.
    constituent_count = membership.groupby('date').sum()[['in_index']].rename(columns={'in_index':
                                                                                       'constituent_count'})
    membership_panel = membership_panel.merge(
        constituent_count, on=['date'], how='left')
    membership_panel['constituent_count'] = membership_panel['constituent_count'].fillna(
        0.)

    indexer = (membership_panel.constituent_count >
               0) & np.isnan(membership_panel.in_index)
    membership_panel.loc[indexer, ['in_index']] = 0

    # what if we have two entries with same ['security_key_abbrev', 'security_key_name', 'date']? (happens when cusip/sedol changes)
    # combine the index weights for in_index=0 or in_index=1 entries seperately
    membership_panel = membership_panel.sort_values(['security_key_name', 'security_key_abbrev', 'date',   'in_index',
                                                     'index_weight']).groupby(['security_key_abbrev', 'security_key_name', 'date', 'in_index']).agg(
        {'total_return_index': 'last', 'index_weight': 'sum',  'constituent_count': 'last', 'security_key': 'last'}).reset_index()
    # if we have an entry for in_index=0 and in_index=1 at same PIT, choose in_index=1
    # an example, S&P500 for security_key_abbrev=='44320110' on 2020,3,31 when cusip changed!
    membership_panel = membership_panel.sort_values(['security_key_name', 'security_key_abbrev', 'date',  'in_index']).groupby([
        'security_key_name', 'security_key_abbrev',  'date']).last().reset_index().sort_values('date')

    # on days with nothing in index, use last value to fill in_index NAs. it's a weekend or holiday!
    interpolated_panel = pd.merge_asof(panel[['security_key_name', 'security_key_abbrev', 'security_key', 'date']],
                                       membership_panel[['security_key_name', 'security_key_abbrev', 'date',
                                                         'in_index',
                                                         'index_weight',
                                                         'total_return_index', 'constituent_count']].dropna(
                                           subset=['in_index']),
                                       on='date',
                                       by=['security_key_name',
                                           'security_key_abbrev'],
                                       )
    return interpolated_panel[['security_key_name', 'security_key_abbrev',
                               'security_key', 'date', 'in_index', 'index_weight']]


def get_sp_500_panel(since, until, frequency='BM'):
    return get_sp_panel(since, until, frequency=frequency, index_name='S&P 500 INDEX')


def get_sp_1500_panel(since, until, frequency='BM'):
    return get_sp_panel(since, until, frequency=frequency, index_name='S&P 1500 INDEX - SUPER COMP')


def __sense_check_panel_and_sort(panel, keep_in_index_only):
    """
    There will be cases where there is more than one period a security_key was in the index
    but the infocode changed, so we have effectively two rows in new_panel (one for each)
    so if in_index==1 take that row, otherwise just take any
    """
    sense_check = panel[(panel.in_index == 1) &
                        (~panel.security_key_abbrev.isna())].set_index(
        ['security_key_abbrev', 'security_key_name', 'date'])
    if not sense_check[sense_check.index.duplicated(keep=False)].empty:
        logging.warning('More than one row for a security key and a PIT - {}'.format(
            pd.DataFrame(sense_check[sense_check.index.duplicated(keep=False)],
                         columns=['security_key_abbrev', 'date', 'infocode'])))
    new_panel = panel.sort_values(
        ['security_key_abbrev', 'security_key_name', 'date', 'in_index'], ascending=False).groupby(
        ['security_key_abbrev', 'security_key_name', 'date']).first().reset_index()
    if keep_in_index_only:
        new_panel = new_panel[new_panel.in_index == 1]
    return new_panel


def __match_security_keys_for_missing_rows(panel, timed_match):
    """
    For missing rows, match the security keys back on using the valid dates
    """
    fill_values = pd.DataFrame()
    for d in panel.date.unique():
        dpanel = panel[panel.date == d].copy()
        match = timed_match[(timed_match.startdate <= d) &
                            ((timed_match.enddate >= d) | (timed_match.enddate.isna()))]
        dpanel = dpanel.merge(match, on=['infocode'], how='right')
        fill_values = pd.concat([fill_values, dpanel])
        del dpanel, match
    return fill_values


def __fill_missing_values(panel, fill_values, qad):
    # However, there are some missing values here, for this default to
    # Use the vw_Ds2Mapping table which is not point in time
    fill_values = fill_values.set_index(['infocode', 'date'])
    panel = panel.set_index(['infocode', 'date'])
    missing_panel = panel[~panel.index.isin(
        fill_values.index)].reset_index(drop=False)
    missing_codes = missing_panel.infocode.unique()
    if len(missing_codes) > 0:
        simple_query = qad.get_security_keys_from_infocodes_now(missing_codes)
        # if a one to one mapping from infocode to security_key, use that
        simple_query_unambigious = simple_query[~simple_query.infocode.duplicated(
            keep=False)]
        fill_not_pit = missing_panel.merge(
            simple_query_unambigious, on='infocode', how='left')
        # form a new panel made of the two methods for mapping to security_keys
        new_panel = pd.concat(
            [fill_not_pit, fill_values.reset_index(drop=False)])
        assert new_panel.shape[0] == panel.shape[0]
    else:
        new_panel = fill_values.reset_index(drop=False)
    return new_panel


def get_index_from_datastream(since, until, frequency='BM', index_code=3670, index_name=None,
                              keep_in_index_only=False):
    """Get a panel for an index from QAD's S&P composition tables.
    Defaults to FTSE 100 Constituents.
    :var since: datetime, date to retrieve index from
    :var until: datetime, date to retrieve index to
    :var frequency: str, string denoting frequency to grab index. Pandas formatting from pandas.date_range() min frequency is monthly due to nature of underlying SQL table
    :var index_code: int, datastream index code
    :var index_name: str, index name as per datastream
    Some common options for index_code are
    FTSE 100 = 3670, 250 = 3671, all share = 3455
    Aim all share 3673 
       """
    qad = ResourceManager().qad
    logging.warning(
        "Retrieving from monthly index table only, frequencies below a month are not valid")
    if not keep_in_index_only:
        logging.warning(
            "Will only keep in_index=0 where relevant mapping to cusip and sedol exists")
    if not index_code:
        if not index_name:
            raise ValueError("Needs at least one of index_code or index_name")
        else:
            index_code = qad.datastream_index_code_from_name(index_name)
            if not index_code:
                raise KeyError(f"No matching index code found for '{index_name}'")
    # SQL query which gets all possible equities in the index
    # May throw warning for missing rows (one month in index)
    membership = qad.datastream_index_constituents(since, until, index_code)

    if membership.empty:
        raise KeyError(f"No constituents found in date range for index_code {index_code}")

    dates = pd.date_range(start=since, end=until, freq=frequency)
    dates_df = pd.DataFrame(dates, columns=['date'])

    panel = dates_df.assign(key=1).merge(membership.assign(key=1), on='key').drop('key', 1)
    in_index_condition = (panel['in_index_since'] <= panel['date']) & (panel['date'] <= panel['in_index_until'])
    panel['in_index'] = np.where(in_index_condition, 1, 0)

    # there will be cases where there is more than one period an infocode was in the index
    # so if in_index==1 take that row, otherwise just take any
    panel = panel.sort_values(['infocode', 'date', 'in_index'], ascending=False).groupby(
        ['infocode', 'date']).first().reset_index()

    # vw_securityMappingX table (is point in time) to link infocode to security_key
    timed_match = qad.get_security_keys_from_infocodes(
        membership.infocode.unique())

    fill_values = __match_security_keys_for_missing_rows(panel, timed_match)

    new_panel = __fill_missing_values(panel, fill_values, qad)

    bad_rows = new_panel[(new_panel.security_key_abbrev.isna())
                         & (new_panel.in_index == 1)]
    if not bad_rows.empty:
        logging.warning(
            'bad infocode to security_key mapping  for these infocodes- {}'.format(bad_rows.infocode.unique()))
        logging.warning('or more verbosely - {}'.format(pd.DataFrame(bad_rows,
                        columns=['infocode', 'date'])))

    new_panel = __sense_check_panel_and_sort(new_panel, keep_in_index_only)

    new_panel.drop(columns=['infocode', 'startdate', 'enddate'], inplace=True)
    # now add in full cusip and sedols with the final checksum character
    new_panel.loc[new_panel.security_key_name == 'sedol', 'security_key'] = sedol_abbrev_to_full(
        new_panel.loc[new_panel.security_key_name == 'sedol', 'security_key_abbrev'])
    new_panel.loc[new_panel.security_key_name == 'cusip', 'security_key'] = cusip_abbrev_to_full(
        new_panel.loc[new_panel.security_key_name == 'cusip', 'security_key_abbrev'])
    return new_panel[~new_panel.security_key.isna()]


def get_gic_panel(index='sp_500', gic='', since=None, until=None, frequency='Q', renormalize_weights=True):
    if index == 'sp_500':
        panel = get_sp_500_panel(since=since,
                                 until=until,
                                 frequency=frequency)
    elif index == 'sp_1500':
        panel = get_sp_1500_panel(since=since,
                                  until=until,
                                  frequency=frequency)
    else:
        raise ValueError(f'Index {index} not supported.')
    panel = panel.features.gic_code()
    if gic:
        panel = panel.loc[panel.gic.apply(lambda x: x[:len(gic)]) == gic]

    if renormalize_weights:
        logging.info(
            "Re-normalizing index weights to sum to 1 over requested universe.")
        columns = panel.columns
        panel = panel.merge(panel.groupby('date').sum()[
                            'index_weight'].rename('weight_sum'), on='date')
        panel['index_weight'] = panel['index_weight'] / panel['weight_sum']
        panel = panel[columns]
    return panel


def get_sp_1200_panel(since, until, frequency='BM'):
    """Retrieve the S&P Global 1200 index members between since and until,
    Frequencies found here https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases,
    BM = business month end frequency,
    This is a global index with S&P500, TSX 60 from Canada and international 640."""
    qad = ResourceManager().qad
    membership = qad.get_sp_1200_membership(since, until)
    key_match = qad.get_security_keys_from_gvkeys(membership.gvkey.unique())
    membership = membership.merge(key_match, left_on=['gvkey', 'iid'], right_on=[
                                  'GVKEY', 'IID'], how='left')
    membership_map = defaultdict(list)
    for i, row in membership.iterrows():
        membership_map[row['security_key_abbrev']].append(
            (row['in_index_since'], row['in_index_until']))

    dates = pd.date_range(start=since, end=until, freq=frequency)
    panel = []
    m_temp = membership[['security_key_abbrev',
                         'security_key_name']].drop_duplicates()
    for i in dates:
        for j, r in m_temp.iterrows():
            panel.append([r['security_key_name'], r['security_key_abbrev'], i])
    panel = pd.DataFrame(
        panel, columns=['security_key_name', 'security_key_abbrev', 'date'])
    del m_temp

    def check_if_in_index(row):
        for in_index_since, in_index_until in membership_map[row['security_key_abbrev']]:
            if in_index_since <= row['date'] <= in_index_until:
                return 1
        return 0
    panel['in_index'] = panel.apply(check_if_in_index, axis=1)
    panel = panel.sort_values('date')
    membership = membership.sort_values('in_index_since')
    panel = pd.merge_asof(panel, membership[['security_key_abbrev', 'security_key_name', 'gvkey', 'in_index_since', 'gvkeyx']],
                          left_on='date', right_on='in_index_since', by=['security_key_abbrev', 'security_key_name'])
    index_dict = {'000003': 'S&P 500', '118341': 'S&P/TSX 60',
                  '150918': 'S&P International 640'}
    panel.loc[:, 'source_index'] = panel.gvkeyx.map(index_dict)
    panel.drop(columns=['gvkeyx', 'in_index_since'], inplace=True)
    panel.loc[panel.security_key_name == 'sedol', 'security_key'] = sedol_abbrev_to_full(
        panel.loc[panel.security_key_name == 'sedol', 'security_key_abbrev'])
    panel.loc[panel.security_key_name == 'cusip', 'security_key'] = cusip_abbrev_to_full(
        panel.loc[panel.security_key_name == 'cusip', 'security_key_abbrev'])
    return panel

def search_for_datastream_index(search_term=None):
    """Returns a list of possible indices available from the Datastream source.
    These names can be used in get_index_from_datastream().
    :param search_term: str, fragment of index name to search for. 
        If none given, returns the whole list of indices (large)
    :returns: A DataFrame listing the code, mnemonic and name of all relevant indices.
    """
    qad = ResourceManager().qad
    if search_term:
        search_term=search_term.upper()
    return qad.datastream_index_name_search(search_term=search_term)
