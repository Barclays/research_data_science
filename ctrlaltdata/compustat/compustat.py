import pyodbc
import pandas as pd
import logging

from ..database import SqlReader
from ..config import (COMPUSTAT_CONNECTION_STRING,
                      DATE_STRING_FORMAT_COMPUSTAT
                      )


class Compustat(SqlReader):
    def __init__(self):
        self.connection = pyodbc.connect(COMPUSTAT_CONNECTION_STRING)

    def get_sp_1500_index_membership(self, since, until):
        query = """SELECT
                        dbo.indexcst_his.fromDate as in_index_since,
                        ISNULL(dbo.indexcst_his.thruDate, '{until}') as in_index_until,
                        dbo.security.cusip as security_key,
                        'cusip' as  security_key_name,
                        dbo.security.gvkey as gvkey
                   FROM
                        dbo.idx_index
                   INNER JOIN
                        dbo.indexcst_his
                   ON
                        dbo.indexcst_his.gvkeyx = dbo.idx_index.gvkeyx
                   INNER JOIN
                        dbo.security
                   ON
                        dbo.security.iid = dbo.indexcst_his.iid
                        AND dbo.security.gvkey = dbo.indexcst_his.gvkey
                   WHERE
                        dbo.idx_index.conm = 'S&P 1500 Super Composite'
                        AND ISNULL(dbo.indexcst_his.thruDate, '{until}') >= '{since}'
                 """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                            until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT))

        result = self.query(query)
        result['gvkey'] = result.gvkey.apply(lambda x: str(x) if len(
            str(x)) == 6 else '0' * (6 - len(str(x))) + str(x))
        result['in_index_since'] = pd.to_datetime(result['in_index_since'])
        result['in_index_until'] = pd.to_datetime(result['in_index_until'])
        return result

    def get_sp_500_index_membership(self, since, until):
        query = """SELECT
                        dbo.indexcst_his.fromDate as in_index_since,
                        ISNULL(dbo.indexcst_his.thruDate, '{until}') as in_index_until,
                        dbo.security.cusip as security_key,
                        'cusip' as  security_key_name,
                        dbo.security.gvkey as gvkey
                   FROM
                        dbo.idx_index
                   INNER JOIN
                        dbo.indexcst_his
                   ON
                        dbo.indexcst_his.gvkeyx = dbo.idx_index.gvkeyx
                   INNER JOIN
                        dbo.security
                   ON
                        dbo.security.iid = dbo.indexcst_his.iid
                        AND dbo.security.gvkey = dbo.indexcst_his.gvkey
                   WHERE
                        dbo.idx_index.conm = 'S&P 500 Comp-Ltd'
                        AND ISNULL(dbo.indexcst_his.thruDate, '{until}') >= '{since}'
                 """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                            until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT))

        result = self.query(query)
        result['gvkey'] = result.gvkey.apply(lambda x: str(x) if len(
            str(x)) == 6 else '0' * (6 - len(str(x))) + str(x))
        result['in_index_since'] = pd.to_datetime(result['in_index_since'])
        result['in_index_until'] = pd.to_datetime(result['in_index_until'])

        return result

    def get_sp500_returns(self, since, until):
        query = """
                    SELECT  datadate AS date,
                            prccddiv AS sp_500_total_return_index
                    FROM    dbo.idx_daily
                    WHERE   gvkeyx = '000003'
                    AND     datadate >= '{since}'
                    AND     datadate <= '{until}'
                    ORDER BY datadate;
                """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT))

        result = self.query(query)
        result['date'] = pd.to_datetime(result['date'])
        return result

    def get_sp1500_returns(self, since, until):
        query = """
                    SELECT  datadate AS date,
                            prccddiv AS sp_1500_total_return_index
                    FROM    dbo.idx_daily
                    WHERE   gvkeyx = '031855'
                    AND     datadate >= '{since}'
                    AND     datadate <= '{until}'
                    ORDER BY datadate;
                """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT))

        result = self.query(query)
        result['date'] = pd.to_datetime(result['date'])
        return result

    def get_co_afnd_feature_since_until_by_gvkeys(self, since, until, gvkeys, table=None,
                                                  feature=None, feature_name=None):
        logging.warning("Metrics added from compustat are not point-in-time. They assume values were known on"
                        " the respective period end date.")
        query = """
                SELECT
                    co_afnd1.gvkey as gvkey,
                    co_afnd2.datadate as date,
                    ISNULL({table}.{feature}, 0) as {feature_name}
                FROM
                    co_afnd2
                INNER JOIN
                    co_afnd1
                    ON co_afnd1.gvkey = co_afnd2.gvkey
                    AND co_afnd1.datadate = co_afnd2.datadate

                WHERE
                    co_afnd1.datafmt = 'STD'
                    AND co_afnd1.indfmt = 'INDL'
                    AND co_afnd1.popsrc = 'D'
                    AND co_afnd2.datafmt = 'STD'
                    AND co_afnd2.indfmt = 'INDL'
                    AND co_afnd2.popsrc = 'D'
                    AND co_afnd2.datadate >= '{since}'
                    AND co_afnd2.datadate <= '{until}'
                    AND co_afnd2.gvkey in ({gvkeys})
                ORDER BY
                    co_afnd2.gvkey,
                    co_afnd2.datadate
                ASC
                """.format(table=table,
                           feature=feature,
                           feature_name=feature_name,
                           since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           gvkeys=",".join(["'{}'".format(gvkey) for gvkey in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result.gvkey.apply(lambda x: str(x) if len(
            str(x)) == 6 else '0' * (6 - len(str(x))) + str(x))
        result['date'] = pd.to_datetime(result['date'])
        return result

    def get_sales_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='sale',
                                                                feature_name='sales')
        return result

    def get_common_shares_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd1',
                                                                feature='csho',
                                                                feature_name='common_shares')
        return result

    def get_total_liabilities_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd1',
                                                                feature='lt',
                                                                feature_name='total_liabilities')
        return result

    def get_assets_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd1',
                                                                feature='at',
                                                                feature_name='assets')
        return result

    def get_preferred_stock_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='pstk',
                                                                feature_name='preferred_stock')
        return result

    def get_ebitda_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd1',
                                                                feature='ebitda',
                                                                feature_name='ebitda')
        return result

    def get_r_and_d_expenses_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='xrd',
                                                                feature_name='r_and_d_expenses')
        return result

    def get_sg_and_a_since_until_by_gvkeys(self, since, until, gvkeys):
        r_and_d = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                 until,
                                                                 gvkeys,
                                                                 table='co_afnd2',
                                                                 feature='xrd',
                                                                 feature_name='r_and_d')
        sg_and_a_with_r_and_d = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                               until,
                                                                               gvkeys,
                                                                               table='co_afnd2',
                                                                               feature='xsga',
                                                                               feature_name='sg_and_a_with_r_and_d')
        result = pd.merge(r_and_d, sg_and_a_with_r_and_d, on=['gvkey', 'date'])
        result['sg_and_a'] = result['sg_and_a_with_r_and_d'] - result['r_and_d']
        return result.drop(columns=['sg_and_a_with_r_and_d', 'r_and_d'])

    def get_net_income_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='ni',
                                                                feature_name='net_income')
        return result

    def get_capex_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd1',
                                                                feature='capx',
                                                                feature_name='capex')
        return result

    def get_total_revenue_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='revt',
                                                                feature_name='total_revenue')
        return result

    def get_filing_date_10q_since_until_by_gvkeys(self, since, until, gvkeys, filing_type='10Q'):
        """
        Get the dates of annual filings. For some filings, the time is available as well in the same table.

        For quarterly filings, use the co_idesind table instead.

        :param since: A datetime.datetime representing the panel start date.
        :param until: A datetime.datetime representing the panel end date.
        :param gvkeys: An iterable of gvkeys (str) representing the units in the panel.
        :param filing_type: One of the strings '8K', '10Q', or 'NW'.
        :return: A pandas.DataFrame with the fields 'gvkey', 'date', 'filing_date'.
        """
        query = """
        SELECT
            gvkey,
            datadate as date,
            filedate as filing_date
        FROM
            co_filedate
        WHERE
            popsrc = 'D'
            AND consol = 'C'
            AND datadate >= '{since}'
            AND datadate <= '{until}'
            AND gvkey in ('{gvkeys}')
            AND srctype = '{filing_type}'
        """.format(**{'since': since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                      'until': until.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                      'gvkeys': "','".join([str(gvkey) for gvkey in gvkeys if not str(gvkey) == 'nan']),
                      'filing_type': filing_type})

        result = self.query(query)
        result['date'] = pd.to_datetime(result['date'])
        result['filing_date'] = pd.to_datetime(result['filing_date'])
        return result

    def get_gvkey_to_gic_since_until(self, gvkeys, since, until):
        query = """SELECT
                co_hgic.gvkey as gvkey,
                co_hgic.indfrom as GICSfrom,
                co_hgic.indthru as GICSthru,
                --co_hgic.gsector,
                --co_hgic.ggroup,
                --co_hgic.gind,
                co_hgic.gsubind as gic

                FROM
                    dbo.co_hgic
                WHERE
                    ( ((co_hgic.indfrom >= '{since}') AND (co_hgic.indfrom <= '{until}'))
                    OR ((ISNULL(co_hgic.indthru,'{until}') >= '{since}')  AND (co_hgic.indfrom <= '{since}')) )
                    AND co_hgic.gvkey in ({gvkeys})""".format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                                                              until=until.strftime(
                                                                  DATE_STRING_FORMAT_COMPUSTAT),
                                                              gvkeys=",".join(["'{}'".format(i) for i in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result.gvkey.astype(
            str).apply(lambda x: '{0:0>6}'.format(x))
        result['GICSfrom'] = pd.to_datetime(result['GICSfrom'])
        result['GICSthru'] = pd.to_datetime(result['GICSthru'])
        return result

    def get_gvkey_to_cik_table(self, gvkeys=[], ciks=[]):
        if len(gvkeys) > 0 and len(ciks) > 0:
            query = """SELECT
                    gvkey,
                    cik
                    FROM
                        company
                    WHERE cik IS NOT NULL
                        AND costat='A'
                        AND gvkey in ({gvkeys})
                        AND cik in ({ciks})""".format(gvkeys=",".join(["'{}'".format(i) for i in gvkeys]),
                                                      ciks=",".join(["'{}'".format(i) for i in ciks]))
        elif len(gvkeys) > 0 and len(ciks) == 0:
            query = """SELECT
                    gvkey,
                    cik
                    FROM
                        company
                    WHERE cik IS NOT NULL
                        AND costat='A'
                        AND gvkey in ({gvkeys})""".format(gvkeys=",".join(["'{}'".format(i) for i in gvkeys]))
        elif len(gvkeys) == 0 and len(ciks) > 0:
            query = """SELECT
                    gvkey,
                    cik
                    FROM
                        company
                    WHERE cik IS NOT NULL
                        AND costat='A'
                        AND cik in ({ciks})""".format(ciks=",".join(["'{}'".format(i) for i in ciks]))
        else:
            query = """SELECT
                    gvkey,
                    cik
                    FROM
                        company
                    WHERE cik IS NOT NULL
                        AND costat='A'"""
        result = self.query(query)
        result['gvkey'] = result.gvkey.astype(
            str).apply(lambda x: '{0:0>6}'.format(x))
        result['cik'] = result.cik.astype(
            str).apply(lambda x: '{0:0>10}'.format(x))
        return result

    def get_gics_codes_dictionaries(self):
        top_level_dict = {}
        query = self.query("""select  * from dbo.r_giccd""")

        def convert_to_dict(df, col):
            return df[df.gictype == col].set_index('giccd')['gicdesc'].to_dict()

        top_level_dict['gsector'] = convert_to_dict(query, 'GSECTOR')
        top_level_dict['ggroup'] = convert_to_dict(query, 'GGROUP')
        top_level_dict['gind'] = convert_to_dict(query, 'GIND')
        top_level_dict['gsubind'] = convert_to_dict(query, 'GSUBIND')
        return top_level_dict

    def get_issuer_name(self, gvkeys):
        if len(gvkeys) == 0:
            gvkeys = ['']

        query = """
                SELECT	DISTINCT
                        gvkey,
                        conm AS issuer_name,
                        conml AS issuer_name_cased
                FROM	dbo.company
                WHERE	gvkey IN ({gvkeys})
                """.format(gvkeys=",".join(["{}".format(i) for i in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result['gvkey'].astype(str)
        return result

    def get_ipo_date(self, gvkeys):
        if len(gvkeys) == 0:
            gvkeys = ['']

        query = """
                SELECT	DISTINCT
                        gvkey,
                        ipodate AS ipo_date
                FROM	dbo.company
                WHERE	gvkey IN ({gvkeys})
                """.format(gvkeys=",".join(["{}".format(i) for i in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result['gvkey'].astype(str).str.zfill(6)
        return result

    def get_quarterly_revenue(self, since, until, gvkeys):
        """
        Returns quarterly revenues
        :param since: datetime
        :param until: datetime
        :param gvkeys: list
        :return: pandas dataframe
        """
        query = """
                SELECT
                    datadate AS quarter_end_date, gvkey, revtq
                FROM
                    co_ifndq
                WHERE
                    co_ifndq.datadate >= '{since}'
                AND
                    co_ifndq.datadate <= '{until}'
                AND
                    co_ifndq.gvkey in ({gvkeys})
                ORDER BY
                    co_ifndq.gvkey, co_ifndq.datadate
                    """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                               until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                               gvkeys=",".join(["'{}'".format(gvkey) for gvkey in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result.gvkey.astype(str).apply(lambda x: '{0:0>6}'.format(x))
        result['quarter_end_date'] = pd.to_datetime(result['quarter_end_date'])
        return result

    def get_quarterly_long_term_debt(self, since, until, gvkeys):
        """
        Returns quarterly long term debt
        :param since: datetime
        :param until: datetime
        :param gvkeys: list
        :return: pandas dataframe
        """
        query = """
                SELECT
                    datadate AS date,
                    gvkey,
                    dlttq AS long_term_debt
                FROM
                    co_ifndq
                WHERE
                    co_ifndq.datadate >= '{since}'
                AND
                    co_ifndq.datadate <= '{until}'
                AND
                    co_ifndq.gvkey in ({gvkeys})
                ORDER BY
                    co_ifndq.gvkey, co_ifndq.datadate
                    """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                               until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                               gvkeys=",".join(["'{}'".format(gvkey) for gvkey in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result.gvkey.astype(str).apply(lambda x: '{0:0>6}'.format(x))
        result['date'] = pd.to_datetime(result['date'])
        return result

    def get_cusip_to_gvkey_table(self, since, until, cusips):
        query = """
                SELECT cssecurity.gvkey
                  ,cssecurity.iid
                  ,cssecurity.effdate
                  ,cssecurity.thrudate
                  ,cssecurity.cusip as security_key
                  ,'cusip' as security_key_name
                  ,cssecurity.exchg
                  ,cssecurity.sedol
                  ,cssecurity.tic
                FROM
                  QPS_Compustat.dbo.cssecurity cssecurity
                WHERE
                  cssecurity.cusip in ({cusips})
                  AND cssecurity.thrudate >= '{since}'
                  AND cssecurity.effdate <= '{until}'
                """.format(since=since.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           until=until.strftime(DATE_STRING_FORMAT_COMPUSTAT),
                           cusips=",".join(["'{}'".format(cusip) for cusip in cusips]))
        df = self.query(query)
        df['gvkey'] = df['gvkey'].apply(lambda x: '{0:0>6}'.format(x))
        return df

    def get_operating_cash_flow_since_until_by_gvkeys(self, since, until, gvkeys):
        result = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                until,
                                                                gvkeys,
                                                                table='co_afnd2',
                                                                feature='oancf',
                                                                feature_name='operating_cash_flow')
        return result

    def get_free_cash_flow_since_until_by_gvkeys(self, since, until, gvkeys):
        """
        Returns simple definition of free cash flow (operating cash flow - capex)
        :param since: datetime
        :param until: datetime
        :param gvkeys: list
        :return: pandas dataframe
        """
        operating_cash_flow = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                                      until,
                                                                      gvkeys,
                                                                      table='co_afnd2',
                                                                      feature='oancf',
                                                                      feature_name='operating_cash_flow')
        capex = self.get_co_afnd_feature_since_until_by_gvkeys(since,
                                                               until,
                                                               gvkeys,
                                                               table='co_afnd1',
                                                               feature='capx',
                                                               feature_name='capex')
        result = pd.merge(operating_cash_flow, capex, on=['gvkey', 'date'])
        result['free_cash_flow'] = result['operating_cash_flow'] - result['capex']
        return result.drop(columns=['operating_cash_flow', 'capex'])

    def get_cusip_by_ticker(self, tickers):
        """
        Finds cusips using tickers
        """
        query ="""
                SELECT cusip, tic as ticker
                FROM dbo.security
                WHERE tic in ({tickers})
                """.format(tickers=",".join(["'{}'".format(ticker) for ticker in tickers]))
        return self.query(query)
