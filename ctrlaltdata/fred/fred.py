from ..config import proxy_dict, fred_api_key
from fredapi import Fred
import os


class FredReader(object):
    def __init__(self, use_proxy=True):
        os.environ['FRED_API_KEY'] = fred_api_key
        if use_proxy:
            os.environ['HTTP_PROXY'] = proxy_dict['http']
            os.environ['HTTPS_PROXY'] = proxy_dict['https']

    @staticmethod
    def get_fred_series(since, until, series_name):
        """
        Retrieve series values from FRED. Gets all revisions for the series requested.

        :param since: datetime; Starting date of the returned series
        :param until: datetime; Ending date of the returned series
        :param series_name:str
                    the name of the dataset. Some data sources (IEX, remote_datareader) will
        :return: Pandas dataframe
        """
        fred = Fred()
        data = fred.get_series_all_releases(series_name, realtime_start=since.strftime("%Y-%m-%d"),
                                            realtime_end=until.strftime("%Y-%m-%d"))
        return data

    @staticmethod
    def get_search_fred(text, limit=1000, order_by=None, sort_order=None, filter=None):
        """
        Search for available FRED metrics using free-text search.
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
        :return: Pandas Dataframe with rows of series metadata according to the search text
        """
        fred = Fred()
        search_results = fred.search(text, limit, order_by, sort_order, filter)
        return search_results
