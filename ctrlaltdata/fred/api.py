import pandas as pd
import numpy as np
from ..resource import ResourceManager


class Features():
    def _get_fred_feature(self, series_name):
        """
        Add arbitrary FRED feature using the input symbol.

        List of kwargs available here: https://fred.stlouisfed.org/docs/api/fred/release_series.html

        :param series_name: FRED series name
        :return:
        """
        fred = ResourceManager().fred
        since, until = self._get_date_range()
        return fred.get_fred_series(since, until, series_name)

    def _return_fred_feature(self, series):
        date_panel = self._obj[['date']].drop_duplicates()
        series_pit = series.loc[series.groupby('date')['realtime_start'].idxmin()].reset_index(drop=True)
        series_pit = series_pit.rename(columns={'date': 'period_date', 'realtime_start': 'date'})
        return pd.merge_asof(date_panel, series_pit, on='date')

    @staticmethod
    def search_fred(text, limit=1000, order_by=None, sort_order=None, filter=None):
        """
        Do a fulltext search for series in the Fred dataset. Returns information about matching series in a DataFrame.
        Use the `series id` column values to input as `series_name` for building new FRED features.

        text : str
            text to do fulltext search on, e.g., 'Real GDP'
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'
        filter : tuple, optional
            filters the results. Expects a tuple like (filter_variable, filter_value).
            Valid filter_variable values are 'frequency', 'units', and 'seasonal_adjustment'

        :return:
        """
        fred = ResourceManager().fred
        return fred.get_search_fred(text, limit, order_by, sort_order, filter)

    def initial_claims(self):
        """
        This function returns the time series for Initial Claimed filed to the U.S. Employment and
        Training Administration as it was available originally.
        The data can also be viewed at https://fred.stlouisfed.org/series/ICSA

        An initial claim is a claim filed by an unemployed individual after a separation from an employer.
        The claim requests a determination of basic eligibility for the Unemployment Insurance program.

        Units: Number, Seasonally Adjusted
        Frequency: Weekly, Ending Saturday

        :return: Pandas dataframe
        """
        initial_claims = self._get_fred_feature('ICSA')
        # ICSA has a constant value for realtime start before 2009-05-28
        # Data is released 5 days after, so update realtime start with date + 5 days before 2009-05-28
        initial_claims['realtime_start'] = np.where((initial_claims['date'] < '2009-05-28') &
                                                    (initial_claims['realtime_start'] == '2009-05-28'),
                                                    initial_claims['date'] + pd.DateOffset(days=5),
                                                    initial_claims['realtime_start'])
        return self._return_fred_feature(initial_claims)

    def gdp(self):
        """
        This function returns the time series for Gross Domestic Product available with the
        U.S. Bureau of Economic Analysis.

        Gross domestic product (GDP), the featured measure of U.S. output, is the market value of the
        goods and services produced by labor and property located in the United States.

        The data can also be viewed at https://fred.stlouisfed.org/series/GDP

        Units:  Billions of Dollars, Seasonally Adjusted Annual Rate
        Frequency:  Quarterly

        :return: Pandas dataframe
        """
        gdp = self._get_fred_feature('GDP')
        return self._return_fred_feature(gdp)
