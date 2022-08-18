import pandas as pd


class SqlReader(object):
    def __init__(self):
        self.connection = None

    def query(self, query, verbose=False, coerce_float=True):
        if verbose:
            print(query)
        return pd.read_sql_query(query, self.connection, coerce_float=coerce_float)

    def get_tables(self):
        return pd.read_sql_query("""
                                 SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'
                                 """, self.connection)
