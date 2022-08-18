from statistics import mean
import unittest
import numpy as np
import pandas as pd
import datetime

from ...panel_constructors import get_security_panel


class TestAnalysis(unittest.TestCase):
    def setUp(self):
        self.df = get_security_panel(since=pd.to_datetime('2020-01-01'),
                                     until=pd.to_datetime('2020-12-01'),
                                     frequency='BM',
                                     cusips=[str(i) for i in range(50)])
        np.random.seed(123)
        self.df['test_feature'] = np.random.normal(1., size=len(self.df))
        self.df['group'] = np.random.choice(range(3), size=len(self.df))

    def test_validate(self):
        self.df.analysis._validate(self.df)

        # check exception raised if required columns not found
        self.assertRaises(AttributeError, self.df.analysis._validate, self.df.drop(
            columns=['security_key', 'security_key_name']))

    def test_add_lagged_feature_on_date_by_key(self):
        
        temp = self.df.copy()

        # check a proper lag
        temp = temp.analysis.add_lagged_feature_on_date_by_key('test_feature', lag=1)
        # check that we pick up the lag
        lag_column = 'test_feature_lag(1)'
        assert lag_column in temp.columns
        # check only missing first date's entries
        assert np.all(temp[temp[lag_column].isna()].date == temp.date.min())
        assert len(temp[temp.date == temp.date.min()]) == len(temp[temp[lag_column].isna()])

        # check a backward lag
        temp = temp.analysis.add_lagged_feature_on_date_by_key('test_feature', lag=-1)
        # check that we pick up the lag
        bwd_lag_column = 'test_feature_lag(-1)'
        assert bwd_lag_column in temp.columns
        # check only missing last date's entries
        assert np.all(temp[temp[bwd_lag_column].isna()].date == temp.date.max())
        assert len(temp[temp.date == temp.date.max()]) == len(temp[temp[bwd_lag_column].isna()])

        # check the null lag
        temp = temp.analysis.add_lagged_feature_on_date_by_key('test_feature', lag=0)
        # check that we pick up the lag
        assert 'test_feature_lag(0)' in temp.columns
        # check only missing last date's entries
        assert 0 == len(temp[temp['test_feature_lag(0)'].isna()])

    def test_add_offset_lagged_feature_on_date_by_key(self):
        temp = self.df.copy()

        # check 5 days offset
        temp = temp.analysis.add_offset_lagged_feature_on_date_by_key(feature_name='test_feature',
                                                                      offset=pd.tseries.offsets.DateOffset(days=5))
        assert 'test_feature_lag(<DateOffset: days=5>)' in temp.columns
        # check only missing first date's entries
        assert np.all(temp[temp['test_feature_lag(<DateOffset: days=5>)'].isna()].date == temp.date.min())

        # check negative 5 day offset
        temp = temp.analysis.add_offset_lagged_feature_on_date_by_key(feature_name='test_feature',
                                                                      offset=pd.tseries.offsets.DateOffset(days=-5))
        assert 'test_feature_lag(<DateOffset: days=-5>)' in temp.columns
        # check no missing entries
        assert 0 == len(temp[temp['test_feature_lag(<DateOffset: days=-5>)'].isna()])

        # check 0 offset
        temp = temp.analysis.add_offset_lagged_feature_on_date_by_key(feature_name='test_feature',
                                                                      offset=pd.tseries.offsets.DateOffset(months=0))
        assert 'test_feature_lag(<DateOffset: months=0>)' in temp.columns
        # check entries same as feature column
        assert (temp['test_feature'] == temp['test_feature_lag(<DateOffset: months=0>)']).all()
        
    def test_mean_center(self):
        temp = self.df.copy()

        assert 0.96 < temp.test_feature.mean() < 0.97
        assert np.abs(temp.analysis._mean_center(['test_feature'], temp, key=['group']).mean()['test_feature']) < 1e-8

    def test_standardize(self):
        temp = self.df.copy()
        temp['test_feature'] = 2. * temp['test_feature']

        # confirm standardization is nontrivial
        assert np.all(1.9 < temp.groupby('group').std()['test_feature'])
        assert np.all(2.1 > temp.groupby('group').std()['test_feature'])

        # test standardization
        print(temp.analysis._standardize(['test_feature'],
                                                 temp,
                                                 key=['group']).groupby(['date', 'group']).std()['test_feature'])
        assert np.allclose(temp.analysis._standardize(['test_feature'],
                                                 temp,
                                                 key=['group']).groupby(['date', 'group']).std()['test_feature'],
                           1.,
                           atol=1e-2)

    def test_rank_on_date_by_key(self):
        temp = self.df.copy()
        ranked = temp.analysis.rank_on_date_by_key(feature_name='test_feature',
                                                   key=['group'])

        # check min rank is 1
        assert ranked['test_feature_ranked'].min() == 1
        # check max rank does not exceed group size
        assert ranked['test_feature_ranked'].max() <= temp.groupby('group').size().max()
    
    def test_quantile_on_date_by_key(self):
        temp = self.df.copy()
        quantiled = temp.analysis.quantile_on_date_by_key(feature_name='test_feature',
                                                       key=['group'],
                                                       quantiles=5)

        assert (np.sort(quantiled['test_feature_quantile'].unique()) == list(range(5))).all()
        
        # check for error raised if too few unique values
        self.df['test_feature_bad'] = 1
        self.assertRaises(ValueError, self.df.analysis.quantile_on_date_by_key, 'test_feature_bad')

    def test_center_and_standardize_key_date_by_gic4_in_index(self):
        # check for KeyError raised on absence of gic4 column
        self.assertRaises(KeyError, self.df.analysis.center_and_standardize_key_date_by_gic4_in_index, ['test_feature'])

        temp = self.df.copy()
        temp['gic4'] = temp['group'].copy()
        centered_and_standardized = temp.analysis.center_and_standardize_key_date_by_gic4_in_index(['test_feature'])

        assert np.allclose(centered_and_standardized.groupby(['date', 'gic4']).mean()['test_feature'], 0)
        assert np.allclose(centered_and_standardized.groupby(['date', 'gic4']).std()['test_feature'], 1)

    def test_interpolate_missing_with_mean(self):
        temp = self.df.copy()
        non_null_indices = np.random.choice(range(temp.shape[0] - 1), size=temp.shape[0]//2)
        null_feature_list = ['test_feature_nulls_0_mean', 'test_feature_nulls_1_mean']
        test_means = {}
        for i, feature_name in enumerate(null_feature_list):
            temp[feature_name] = np.nan
            fill_series = np.random.normal(i, size=len(non_null_indices))
            test_means[feature_name] = np.mean(fill_series)
            temp.loc[non_null_indices, feature_name] = fill_series
        
        df_interpolated = temp.analysis.interpolate_missing_with_mean(temp, null_feature_list)

        # Check if mean of null indices is as expected
        for feature_name in null_feature_list:
            assert np.isclose(df_interpolated.loc[~df_interpolated.index.isin(non_null_indices), feature_name].mean(), 
                              test_means[feature_name], atol=1e-1)

    def test_add_time_windowed_functions(self):
        def subtract_min_from_feature(inputs):
            orig_df, date_dict, past_df, present_df, future_df, feature = inputs
            panel_list = [past_df, present_df, future_df]
            for i, panel in enumerate(panel_list):
                panel[feature] = panel[feature] - panel[feature].min()
            return panel_list

        def divide_feature_by_max(inputs):
            orig_df, date_dict, past_df, present_df, future_df, feature = inputs
            panel_list = [past_df, present_df, future_df]
            for i, panel in enumerate(panel_list):
                panel[feature] = panel[feature] / panel[feature].max()
            return panel_list

        temp = self.df.copy()
        temp['test_feature'] = np.random.randint(low=1, high=100000, size=temp.shape[0])
        functions = [subtract_min_from_feature, divide_feature_by_max]
        args = ['test_feature']
        windowed_results = temp.analysis.apply_time_windowed_functions(functions, args, sc=None, partitions=None,
                                                                       prediction_steps=1, lookback=datetime.timedelta(days=100 * 365),
                                                                       train_lt='', burn_in_steps=0, df=temp, n_jobs=10)
        test_feature_subtraction = windowed_results['subtract_min_from_feature']
        test_feature_division = windowed_results['divide_feature_by_max']

        # verify shape of output
        assert len(windowed_results.keys()) == 2
        assert len(test_feature_subtraction) == len(test_feature_division) == 9
        assert all([len(item) == 3 for item in test_feature_subtraction]) and all([len(item) == 3 for item in test_feature_division])

        # verify dataframe date alignments and correct function application
        for function in functions:
            for time_step in range(9):
                past_df, present_df, future_df = windowed_results[function.__name__][time_step]
                assert past_df['date'].max() <= present_df['date'].unique()[0] < future_df['date'].unique()[0]
                if function.__name__ == 'subtract_min_from_feature':
                    assert past_df['test_feature'].min() == present_df['test_feature'].min() == future_df['test_feature'].min() == 0
                else:
                    assert past_df['test_feature'].max() == present_df['test_feature'].max() == future_df['test_feature'].max() == 1


if __name__ == '__main__':
    unittest.main()
