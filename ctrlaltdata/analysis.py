import logging
import pandas as pd
import datetime
import tqdm
from pandas.tseries.offsets import DateOffset

from .config import spark_installed

if spark_installed:
    import pyspark


@pd.api.extensions.register_dataframe_accessor("analysis")
class AnalysisAccessor(object):
    def __init__(self, pandas_obj):
        """
        Expects a dataframe with unique columns on ['cusip', 'date'] pairs.
        """
        self._validate(pandas_obj)
        self._obj = pandas_obj

    def _validate(self, obj):
        """
        Validate that we have all required fields for the analysis accessor: a `date`, `security_key`, and
        `security_key_name` field.

        :param self: the analysis accessor.
        :param obj: The pandas.DataFrame to validate for use with the analysis accessor.
        :returns: No return value.
        """
        if 'security_key' not in obj.columns and 'security_key_name' not in obj.columns:
            raise AttributeError("Must have security key columns.")
        if 'date' not in obj.columns:
            logging.warning("No 'date' in dataframe. Won't be able to asof merge.")

    def add_lagged_feature_on_date_by_key(self, feature_name, key=['security_key', 'security_key_name'], lag=1, freq=None):
        """
        Lag a feature given by the `feature_name` for each security (indicated by the `key`) along the panel's time
        dimension by a discrete number of time steps. Choosing a row of data corresponding to the present, a lag of 1
        would put the panel's last time point's value onto the present time point. A lag of -1 would put the next time
        point's value onto the present row.

        Pandas's `shift` is a little tricky, since it doesn't always respect time ordering and indexing. This method
        makes time lagging by security more failure resistant.

        :param feature_name: A string indicating the name of the feature to shift.
        :param key: A list of strings indicating the multi-key describing the unit of analysis.
        :param lag: An integer indicating the number of panel time steps to lag the feature.
        :param freq: A pass-through for the `pandas.DataFrame`'s `shift` method's `freq` argument.
        :return self._obj: The dataframe with a new column containing the shifted values. The new column name is the
        same as the old column name, but with `f'_lag({lag})'` appended to it.
        """
        self._obj = self._obj.sort_values('date')
        self._obj.loc[:, f'{feature_name}_lag({lag})'] = pd.Series(self._obj[feature_name], index=self._obj.index)
        def shift_column(df, column=f'{feature_name}_lag({lag})', shift=lag):
            df.loc[:, column] = pd.Series(df[column].shift(shift, freq=freq), index=df.index)
            return df
        self._obj = self._obj.groupby(key).apply(shift_column).reset_index(drop=True) # [TODO: throws a warning. fix it!]
        return self._obj

    def add_offset_lagged_feature_on_date_by_key(self, feature_name, key=['security_key', 'security_key_name'],
                                                 offset=pd.tseries.offsets.DateOffset(months=1)):
        """
        Lag a feature given by the `feature_name` for each security (indicated by the `key`) along the panel's time
        dimension by a fixed offset. When the lagged value is from a time that falls between two panel time steps, it
        will be asof merged back onto the panel, thus aligning it to the later of the two time steps. If the offset is
        less than the length of a panel period, then this may result in no shifting!

        Pandas's `shift` is a little tricky, since it doesn't always respect time ordering and indexing. This method
        makes time lagging by security more failure resistant.

        :param feature_name: A string indicating the name of the feature to shift.
        :param key: A list of strings indicating the multi-key describing the unit of analysis.
        :param offset: A `pandas.tseries.offset` or similar object (e.g. a timedelta) which indicates the lag period.
        :param freq: A pass-through for the `pandas.DataFrame`'s `shift` method's `freq` argument.
        :return self._obj: The dataframe with a new column containing the shifted values. The new column name is the
        same as the old column name, but with `f'_lag({offset})'` appended to it.
        """
        logging.warning("Caution! Month lagging with DateOffset can be unintuitive near month ends.")
        shifted = self._obj[key + ['date', feature_name]].copy()
        shifted.rename(columns={feature_name: f'{feature_name}_lag({offset})'}, inplace=True)
        shifted.loc[:,'date'] = [i + offset for i in shifted.loc[:, 'date']]
        return self._obj.features._asof_merge_feature(shifted, f'{feature_name}_lag({offset})', by=key)

    def _mean_center(self, feature_names, subpanel, key=['gic4']):
        """
        This is an opinionated internal method for mean-centering features of a group of securities within a defined
        sub-universe, where the mean calculation is restricted to be within the subset of that which is also in the
        investment universe. It is most often used for point-in-time ML operations, where the subpanel represents a
        time slice of the main panel.

        :param feature_names: A list of strings indicating which columns of the subpanel to mean-center.
        :param subpanel: A `pandas.DataFrame` which is a valid panel to be mean-centered.
        :param key: A list containing a multi-key defining groups of unit keys to be centered to the same mean.
        :returns centered_panel: A `pandas.DataFrame` containing the centered panel.
        """
        subpanel = subpanel.copy()
        if type(key) == str:
            key = [key]
        panel_mean = subpanel[subpanel.in_index == 1].groupby(key + ['date']).mean()
        panel_mean = panel_mean.rename(columns={col: col + '_mean' for col in panel_mean.columns})

        centered_panel = subpanel.copy()
        centered_panel = centered_panel.set_index(key + ['date'], drop=True)

        centered_panel = centered_panel.merge(panel_mean, on=key + ['date'])
        centered_panel[feature_names] = centered_panel[feature_names] - \
                                        centered_panel[[col + '_mean' for col in feature_names]] \
                                            .rename(columns={col + '_mean': col for col in feature_names})
        for col in panel_mean.columns:
            del centered_panel[col]

        return centered_panel.reset_index()

    def _standardize(self, feature_names, subpanel, key=['gic4']):
        """
        This is an opinionated internal method for standardizing features of a group of securities within a defined
        sub-universe, where the standard deviation calculation is restricted to be within the subset of that which is
        also in the investment universe. It is most often used for point-in-time ML operations, where the subpanel
        represents a time slice of the main panel.

        :param feature_names: A list of strings indicating which columns of the subpanel to standardize.
        :param subpanel: A `pandas.DataFrame` which is a valid panel to be standardized.
        :param key: A list containing a multi-key defining groups of unit keys to be standardized to the same stddev.
        :returns standardized_panel: A `pandas.DataFrame` containing the standardized panel.
        """
        subpanel = subpanel.copy()
        if type(key) == str:
            key = []

        for feature_name in feature_names:
            def std(series):
                series[:] = series.std()
                return series

            subpanel = subpanel.analysis.apply_f_on_date_by_key(feature_name,
                                                                  std,
                                                                  key=key + ['in_index'],
                                                                  suffix='_std')
            subpanel[feature_name] = subpanel[feature_name] / subpanel[f'{feature_name}_std']
            del subpanel[f'{feature_name}_std']
        return subpanel

    def apply_f_on_date_by_key(self, feature_name, f, key=[], prefix='', suffix='_f', *args, in_index=True, **kwargs):
        """
        It can be useful to apply a function to values of a feature defined over a group of securities at a point in
        time, then join the result of that function onto the panel. Examples include adding a column indicating the
        industry standard deviation of a feature (say, sales) at a point in time to be used for standardization, or
        the Herfindahl index computed from industry sales as a measure of industry concentration.

        :param feature_name: The name of the column on which to compute the feature.
        :param f: The function to apply to a past, present, future slice of data.
        :param key: The group key to compute the feature. This will be used together with in_index to run the function
        in the universe and outside of it separately.
        :param prefix: A prefix for the new column containing the computed feature. The full name will be
        `prefix + feature_name + suffix`
        :param suffix: The suffix for the new column containing the computed feature.
        :param *args: Additional args will be passed through to the function at evaluation.
        :param in_index: Defaults to True. Whether to compute the new column in the index and outside of it. If False,
        skips computing it when `in_index==0`.
        :param **kwargs: kwargs to pass through to the function at evaluation.
        :returns panel: The original panel with the new feature in a column called `prefix + feature_name + suffix`.
        """
        if in_index:
            indexer = (self._obj.in_index == 1)
        else:
            indexer = self._obj.index
        group_key = self._obj.features.time_key + key
        result = self._obj.loc[indexer].groupby(group_key).apply(lambda x: f(x[feature_name], *args, **kwargs))
        self._obj.loc[indexer, prefix + feature_name + suffix] = \
        result.reset_index(level=[i for i in range(len(group_key))])[feature_name]
        return self._obj

    def rank_on_date_by_key(self, feature_name, key=[], ascending=True, in_index=True):
        """
        Rank securities within groups defined by the `key` at each point in time. If in_index is True, the ranking
        will only be over securities which are in the index (based on the indicator variable) at the point in time.

        This will return the original panel with a new column called `f'{feature_name}_ranked'` containing the ranks.

        :param feature_name: The name of the feature to use for ranking.
        :param key: A list of strings representing a multi-key. This defines groups to rank within.
        :param ascending: A boolean indicating whether the rank should be based on ascending or descending feature
        values. This defaults to ascending (True).
        :param in_index: A boolean indicating whether to rank within the index only, or over all securities. Defaults to
        True.
        :returns df: The panel with the rank column added to it.
        """
        def f(column):
            column = column.rank(ascending=ascending).astype(int)
            return column

        return self.apply_f_on_date_by_key(feature_name, f, key=key, suffix='_ranked', in_index=in_index)

    def quantile_on_date_by_key(self, feature_name, key=['in_index'], in_index=True, quantiles=5):
        """
        Quantile securities by a feature within groups defined by the `key` at each point in time. If in_index is
        True, the quantiling  will only be over securities which are in the index (based on the indicator variable) at
        the point in time.

        This will return the original panel with a new column called `f'{feature_name}_quantile'` indicating the quantile
        into which the row falls as an integer in `range(quantiles)`.

        :param feature_name: The name of the feature to use for quantiling.
        :param key: A list of strings representing a multi-key. This defines groups to quantile within.
        :param in_index: A boolean indicating whether to rank within the index only, or over all securities. Defaults to
        True.
        :param quantiles: The number of quantiles to use, an integer. This defaults to 5.
        :returns df: The panel with the quantile column added to it.
        """
        def quantile(column, k=quantiles):
            return pd.qcut(column, k, labels=range(k)).astype(int)

        return self.apply_f_on_date_by_key(feature_name, quantile, key=key, suffix='_quantile', in_index=in_index)

    def center_and_standardize_key_date_by_gic4_in_index(self, feature_names):
        """
        Mean-centers and standardizes features named in the list `feature_names` within each gic4 within the analysis
        universe.

        :param feature_names: A list of strings indicating the features to center and standardize.
        :returns df: The panel with the given `feature_names` centered and standardized.
        """
        subpanel = self._obj.copy()
        subpanel = self._standardize(feature_names, subpanel)
        subpanel = self._mean_center(feature_names, subpanel)
        return subpanel

    def apply_time_windowed_functions(self, functions, args, sc=None, partitions=None,
                                      prediction_steps=1, lookback=datetime.timedelta(days=100*365),
                                      train_lt='', burn_in_steps=0, df=None, n_jobs=10):
        """
        Often we need to perform an analysis at a single point in time. We can run through a panel examining each time
        point as if it were the "present", and run an analysis at that point in time. From that time point, we have a
        well-defined past (everything with panel date less than or equal to the present date), present (everything in
        the panel with panel date equal to the present date) and future (everything in the panel with date greater than
        the present). Note that if using e.g. closing prices which arrive at 5pm, and making predictions during the day
        at an earlier time, you'd need to exclude this intra-day future information using the `'train_lt'` field.

        Common operations with this structure include computing portfolio weights, predictive modeling, and more. For
        example, we might train a model using past data, and use the present data to try to predict the future. We can
        compare those predictions with the actual future outcomes to see how well we did in a form of "out-of-time" (as
        opposed to out-of-sample) cross-validation.

        We control the past time window using the `lookback` parameter. This defines how far into the past from the
        `present` we look during this process. We control the future time point using the `prediction_steps` parameter.
        It indicates how many steps into the future we care to look. We control the earliest time we'd like to call the
        "present" using the `burn_in_steps` parameter. If we start too early, there won't be enough past to examine. It
        is common to choose `burn_in_steps` long enough to ensure the first "present" has at least `lookback` past
        steps.

        In the case that the present can contain future information (this is a dangerous practice!), `train_lt` is used
        to name a column of a panel containing the latest future information on the present data. This prevents future
        contamination while training on the past.

        These operations are not vectorized and can be very expensive. To improve runtime, we've added parallelization
        with PySpark which will run whenever the user has configured `spark_installed=True` in their configuration.
        Otherwise, the functions are applied serially.

        The functions to be applied at the present should accept at least two arguments: the original panel, and the
        date dictionary as produced by `panel.analysis.generate_dates`. Then, it can optionally take any number of args
        provided by the user in `args`.

        :param functions: A list of functions to be applied.
        :param args: A list of additional args to supply to the function (after required args).
        :param sc: The SparkContext when using PySpark
        :param partitions: The number of partitions to use when using PySpark (deprecated, replace with n_jobs).
        :param prediction_steps: The number of steps ahead to consider the future time.
        :param lookback: A pd.tseries.offsets or datetime.timedelta object to define the farthest back the past can go.
        Defaults to 100 years, effectively making this an expanding-window past.
        :param train_lt: A string for the column indicating the latest time point at which the present contains future
        information. Note that this is defined for each row of the panel.
        :param burn_in_steps: The number of time steps to wait before applying functions.
        :param df: The panel on which to run the analysis.
        :param n_jobs: The number of processes to use for parallelization. In PySpark, this will be the number of
        partitions to use.
        :returns result: The list of return values for each time slice of the panel.
        """
        if spark_installed:
            if not sc:
                sc = pyspark.SparkContext.getOrCreate()
            df = sc.broadcast(df)
            return self.apply_time_windowed_functions_spark(functions, args, sc=sc, partitions=partitions,
                                      prediction_steps=prediction_steps, lookback=lookback,
                                      train_lt=train_lt, burn_in_steps=burn_in_steps, df=df, n_jobs=n_jobs)
        else:
            return self.apply_time_windowed_functions_serial(functions, args, sc=sc, partitions=partitions,
                                      prediction_steps=prediction_steps, lookback=lookback,
                                      train_lt=train_lt, burn_in_steps=burn_in_steps, df=df, n_jobs=n_jobs)
    
    def apply_time_windowed_functions_spark(self, functions, args, sc=None, partitions=None,
                                      prediction_steps=1, lookback=datetime.timedelta(days=100*365),
                                      train_lt='', burn_in_steps=0, df=None, n_jobs=10):
        logging.info("using spark version.")
        dates = sc.parallelize([([df, date_info], [df, date_info]) for date_info in self.generate_dates(prediction_steps=prediction_steps, lookback=lookback,
                                  train_lt=train_lt, burn_in_steps=burn_in_steps) if date_info], n_jobs)
        to_run = dates.mapValues(self.temporal_split_panel).map(lambda x: list(x[0]) + list(x[1]) + list(args))
        results = {}
        for function in functions:
            results[function.__name__] = to_run.map(function).collect()
        return results

    def apply_time_windowed_functions_serial(self, functions, args, sc=None, partitions=None,
                                      prediction_steps=1, lookback=datetime.timedelta(days=100*365),
                                      train_lt='', burn_in_steps=0, df=None, n_jobs=10):
        logging.info("using serial version.")
        dates = [[df, date_info] for date_info in self.generate_dates(prediction_steps=prediction_steps, lookback=lookback,
                                  train_lt=train_lt, burn_in_steps=burn_in_steps) if date_info]
        to_run = list(map(lambda x: list(x[0]) + list(x[1]) + args,
                     [(t[:2], x) for t, x in zip(dates, map(self.temporal_split_panel, dates))]))
        results = {}
        for function in functions:
            logging.info(f"running {function.__name__}.")
            results[function.__name__] = [i for i in tqdm.tqdm(map(function, to_run))]
        return results

    def generate_dates(self, prediction_steps=1, lookback=datetime.timedelta(days=365 * 100),
                                  train_lt='', burn_in_steps=0):
        """
        This method is used by our point-in-time analysis tools to generate a sequence of dates on which to slice a
        panel. It returns a map containing, for each time step to be analyzed (considered the "present"), the present
        date, the beginning and end dates that define the past, and the future date. It also contains the train_lt date,
        which is the latest future information contained in the present data.

        :param prediction_steps: The number of steps ahead to consider the future time.
        :param lookback: A pd.tseries.offsets or datetime.timedelta object to define the farthest back the past can go.
        Defaults to 100 years, effectively making this an expanding-window past.
        :param train_lt: A string for the column indicating the latest time point at which the present contains future
        information. Note that this is defined for each row of the panel.
        :param burn_in_steps: The number of time steps to wait before applying functions.
        :returns date_info: This generates a dictionary containing the fields indicated above.
        """
        dates = [pd.to_datetime(d) for d in sorted(self._obj.date.unique())]
        for i, present_date in enumerate(dates):
            # burn in, make sure there is a future, and make sure there are outcomes in the history
            if i >= burn_in_steps and len(dates) > i + prediction_steps and i - prediction_steps >= 0:
                past_end = dates[i]
                past_begin = past_end - lookback
                future_date = dates[i + prediction_steps]
                if present_date > dates[0]:
                    yield {"present_date": present_date,
                           "past_begin": past_begin,
                           "past_end": past_end,
                           "future_date": future_date,
                           "train_lt": train_lt}

    @staticmethod
    def temporal_split_panel(x):
        df, date_info = x
        if spark_installed:
            df = df.value
        past = df[(df.date <= date_info["past_end"]) & (df.date >= date_info["past_begin"])].copy()
        if date_info["train_lt"]:  # account for lookahead information in the indep vars
            past = past[past[date_info["train_lt"]] < date_info["past_end"]]
        present = df[df.date == date_info["present_date"]].copy()
        future = df[df.date == date_info["future_date"]].copy()
        return past, present, future

    def interpolate_missing_with_mean(self, df, variable_names):
        """
        This accepts a dataframe and list of variable names. For each variable, it will interpolate the mean of that
        variable into the missing data fields. This is used most often alongside our point-in-time ML tools to
        interpolate a training set of past data prior to fitting a model.

        This does not restrict to in_index==1, so should be used carefully when the panel contains out-of-index
        securities. This checks each column to ensure it is a numeric type, and passes without interpolating when it
        isn't.

        :param df: the panel to interpolate.
        :param variable_names: the list of variable names to be interpolated.
        :returns df: The panel with the listed variables interpolated.
        """
        for variable in variable_names:
            if pd.api.types.is_numeric_dtype(df.loc[:, variable]):
                df.loc[:, [variable]] = df[variable].fillna(df[variable].mean())
        return df
