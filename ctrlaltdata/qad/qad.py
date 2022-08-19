import pyodbc
import pandas as pd
import numpy as np
import logging

from ..database import SqlReader
from ..util import (clean_string,
                    convert_metric_to_currency_aware_column)
from ..config import (QAD_CONNECTION_STRING,
                      DATE_STRING_FORMAT_QAD
                      )


class QAD(SqlReader):
    def __init__(self):
        self.connection = pyodbc.connect(QAD_CONNECTION_STRING)

    def get_daily_sp_index_membership(self, since, until, index_name='S&P 500 INDEX'):
        query = """
        SELECT
            index_security.Cusip,
            mast.Cusip as security_key_abbrev,
            composition.Date_ as "date",
            composition.Weight as "index_weight",
            'cusip' as security_key_name,
            index_security.FirstDate as in_index_since,
            index_security.LastDate as in_index_until,
            ds2.RI total_return_index
        FROM 
            dbo.IdxSpCmp composition
        INNER JOIN
            dbo.IdxInfo index_info ON composition.IdxCode = index_info.Code
        LEFT JOIN
            prc.IdxSec index_security ON composition.SecCode = index_security.Code
            AND index_security.Vendor = 1
        LEFT JOIN
            dbo.vw_SecurityMasterX mast on mast.SecCode = index_security.PrcCode
            AND mast.typ = 1 -- north american only
        LEFT JOIN 
            dbo.vw_SecurityMappingX mapping ON mast.SecCode = mapping.seccode
           AND mast.typ = mapping.typ
           AND mapping.ventype = 33
           AND mapping.rank = 1
        LEFT JOIN 
            Ds2PrimQtRI ds2 ON ds2.infocode = mapping.vencode
            AND ds2.MarketDate = composition.Date_

        WHERE
            index_info.Type_ = 1
            AND index_info.Name = '{index_name}'
            AND composition.Date_ >= '{since}'
            AND composition.Date_ <= '{until}'
            AND index_security.Cusip IS NOT NULL
        ORDER BY
            composition.Weight
        DESC
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   index_name=index_name)
        return self.query(query)

    def get_return_index_since_until_by_cusip_sedol(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """SELECT distinct 
        mastX.typ,
		CASE WHEN mastX.cusip IS NOT NULL THEN
			CASE
				WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			ELSE mastX.prevcusip END 
		ELSE
			CASE
				WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			ELSE mastX.prevSedol END
		END AS security_key_abbrev,
		CASE 
            WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
            ELSE 'sedol'
        END AS security_key_name,
        RI.marketdate AS date,
        RI.RI AS total_return_index

        FROM Ds2PrimQtRI RI

        INNER JOIN vw_securityMappingX mapX
            ON RI.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
        and MapX.Rank=1
            AND RI.marketdate > '{since}'
            AND RI.marketdate < '{until}'
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}) )
            OR (mastX.cusip IN ({cusips}) OR mastX.prevcusip IN ({cusips}))
                )""".format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                            until=until.strftime(DATE_STRING_FORMAT_QAD),
                            cusips=",".join(["'{}'".format(i) for i in cusip]),
                            sedols=",".join(["'{}'".format(i) for i in sedol]))
        result = self.query(query)
        result['date'] = pd.to_datetime(result['date'])
        keep = ['security_key_abbrev', 'security_key_name',
                'date', 'total_return_index']
        return result[keep]

    def get_security_keys_from_gvkeys(self, gvkey):

        ROW_query = """
        select 
            map.seccode,
            map.typ, 
            map.startdate, 
            map.enddate,  
            mast.sedol as security_key_abbrev,
            'sedol' as security_key_name,
            ROW.GVKEY,
            ROW.IID        
        from dbo.vw_SecurityMappingX map
            left join dbo.CSGSec ROW
                on ROW.SECID=map.vencode
                AND map.ventype=4
            join dbo.vw_SecurityMasterX mast
                on mast.seccode=map.seccode
                and mast.typ=map.typ
            where 
                ROW.gvkey in ({gvkeys}) """.format(gvkeys=",".join(["'{}'".format(i) for i in gvkey]))
        NA_query = """
        select 
            map.seccode,
            map.typ, 
            map.startdate, 
            map.enddate,  
            mast.cusip as  security_key_abbrev,
            'cusip' as security_key_name,
            NA.GVKEY,
            NA.IID        
        from dbo.vw_SecurityMappingX map
            join dbo.csvsecurity NA
                on NA.SECINTCODE=map.vencode
                AND map.ventype=4
            join dbo.vw_SecurityMasterX mast
                on mast.seccode=map.seccode
                and mast.typ=map.typ
            where 
                NA.gvkey in ({gvkeys}) """.format(gvkeys=",".join(["'{}'".format(i) for i in gvkey]))
        combined = pd.concat(
            [pd.read_sql(NA_query, self.connection), pd.read_sql(ROW_query, self.connection)])
        combined.loc[:, 'GVKEY'] = combined['GVKEY'].astype(
            str).apply(lambda x: '{0:0>6}'.format(x))
        return combined

    def get_seccode_by_keys(self, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """SELECT distinct 
        mastX.typ,
        mastX.seccode,
        CASE WHEN mastX.cusip IS NOT NULL THEN
            CASE
                WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
            ELSE mastX.prevcusip END 
        ELSE
            CASE
                WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
            ELSE mastX.prevSedol END
        END AS security_key_abbrev,
        CASE 
            WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
            ELSE 'sedol'
        END AS security_key_name

        FROM vw_SecurityMasterX mastX

        WHERE    ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}) )
            OR (mastX.cusip IN ({cusips}) OR mastX.prevcusip IN ({cusips}))
                )""".format(cusips=",".join(["'{}'".format(i) for i in cusip]),
                            sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def get_ticker_by_cusip(self, cusip):
        if len(cusip) == 0:
            cusip = ['']
        query = """select distinct
            cusip,
            tic as ticker
            from CSVSecurity
           WHERE cusip in ({cusips})""".format(cusips=",".join(["'{}'".format(i) for i in cusip]))
        return self.query(query)

    def get_exchange_rate_since_until_by_currency_codes(self, since, until, from_currency, to_currency):
        query = """
        SELECT
            fxcode.fromcurrcode AS from_currency ,
            fxcode.tocurrcode AS to_currency,
            fxrate.exratedate AS date,
            fxrate.midrate AS exchange_rate

        FROM DS2FXCode fxcode

        LEFT JOIN dbo.DS2FXRate fxrate
              ON fxcode.ExRateIntCode = fxrate.ExRateIntCode

        WHERE (fxcode.RateTypeCode = 'SPOT')
              AND fxrate.exratedate > '{since}'
              AND fxrate.exratedate < '{until}'
              AND  fxcode.fromcurrcode IN ({from_currency})
              AND  fxcode.tocurrcode IN ({to_currency}) 
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   from_currency=",".join(["'{}'".format(i)
                                           for i in from_currency]),
                   to_currency=",".join(["'{}'".format(i) for i in to_currency]))
        # not all currency pairs exist in both directions in the database, i.e from SEK to USD exists but not USD to SEK
        # so perform a second query reversing to and from, invert the values and the to/from labels
        query_invert = """
        SELECT
            fxcode.tocurrcode AS from_currency ,
            fxcode.fromcurrcode AS to_currency,
            fxrate.exratedate AS date,
            1/fxrate.midrate AS exchange_rate

        FROM DS2FXCode fxcode
        LEFT JOIN dbo.DS2FXRate fxrate
              ON fxcode.ExRateIntCode = fxrate.ExRateIntCode

        WHERE (fxcode.RateTypeCode = 'SPOT')
              AND fxrate.exratedate > '{since}'
              AND fxrate.exratedate < '{until}'
              AND  fxcode.fromcurrcode IN ({to_currency}) 
              AND  fxcode.tocurrcode IN ({from_currency}) 
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   from_currency=",".join(["'{}'".format(i)
                                           for i in from_currency]),
                   to_currency=",".join(["'{}'".format(i) for i in to_currency]))
        main_query = self.query(query).set_index(
            ['from_currency', 'to_currency', 'date']).dropna(subset=['exchange_rate'])
        invert_query = self.query(query_invert).set_index(
            ['from_currency', 'to_currency', 'date']).dropna(subset=['exchange_rate'])
        # filter out the values that exist in the main query
        invert_query = invert_query[~invert_query.index.isin(main_query.index)]
        result = pd.concat(
            [main_query.reset_index(), invert_query.reset_index()])
        result['date'] = pd.to_datetime(result['date'])
        return result
    
    def get_consolidated_share_count_since_until_by_cusip_sedol(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT

            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            CA.VALDATE as date,
            CA.ConsolNumShrs*1000 as consolidated_share_count
        FROM dbo.DS2Mktval CA

        INNER JOIN vw_securityMappingX mapX
            ON CA.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND CA.VALDATE > '{since}'
            AND CA.VALDATE < '{until}'
            AND MapX.rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def get_market_value_since_until_by_cusip_sedol(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT

            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            CA.VALDATE as date,
            CA.currency as mkt_val_currency,
            CA.ConsolMktVal as consolidated_market_value
        FROM dbo.vw_Ds2ConsolidatedMktCap CA

        INNER JOIN vw_securityMappingX mapX
            ON CA.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND CA.VALDATE > '{since}'
            AND CA.VALDATE < '{until}'
            AND MapX.rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)
    
    def get_market_cap_since_until_by_cusip_sedol(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT

            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            CA.marketdate as date,
            CA.currency as mkt_cap_currency,
            CA.Mktcap as market_cap
        FROM dbo.vw_Ds2MktCap CA

        INNER JOIN vw_securityMappingX mapX
            ON CA.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND CA.MARKETDATE > '{since}'
            AND CA.MARKETDATE < '{until}'
            AND MapX.rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)
    
    def get_free_float_market_cap_since_until_by_cusip_sedol(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT

            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            CA.marketdate as date,
            CA.currency as mkt_cap_currency,
            CA.FreeFloatMktCap as free_float_market_cap
        FROM dbo.vw_Ds2FreeFloatMktCap CA

        INNER JOIN vw_securityMappingX mapX
            ON CA.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND CA.MARKETDATE > '{since}'
            AND CA.MARKETDATE < '{until}'
            AND MapX.rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def get_closing_price_since_until_by_cusip_sedol(self, since, until, adj_type, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT
            CASE 
                WHEN mastX.cusip IS NOT NULL THEN
			        CASE
				        WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			            ELSE mastX.prevcusip END 
		        ELSE
			        CASE
				            WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            pricing.close_ AS datastream_closing_price,
            pricing.marketdate AS date,
            pricing.AdjType,
            pricing.currency as datastream_currency
        FROM vw_Ds2Pricing pricing

        INNER JOIN vw_securityMappingX mapX
            ON pricing.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND pricing.marketdate > '{since}'
            AND pricing.marketdate < '{until}'
            AND pricing.adjtype = {AdjTyp}
            AND pricing.IsPrimExchQt = 'Y'
            and MapX.Rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   AdjTyp=adj_type,
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def get_common_shares_one_listing(self, since, until, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        # note that Datastream gives share  in units of thousands so divide by 1000
        query = """
        SELECT

            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            DS.EVENTDATE as date,
            DS.NumShrs /1000 as common_shares

        FROM dbo.Ds2NumShares DS


        INNER JOIN vw_securityMappingX mapX
            ON DS.infocode = mapX.vencode

        INNER JOIN vw_SecurityMasterX mastX
            ON mastX.seccode = mapX.seccode
            AND mastX.typ = mapX.typ

        WHERE mapX.ventype = 33    --the datastream ventype
            AND DS.EVENTDATE > '{since}'
            AND DS.EVENTDATE < '{until}'
            AND MapX.rank=1
            AND ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
        """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                   until=until.strftime(DATE_STRING_FORMAT_QAD),
                   cusips=",".join(["'{}'".format(i) for i in cusip]),
                   sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def lookup_ibes_metrics(self):
        query = """
                SELECT  tre1.code,
                        tre1.description AS code_name,
                        tre2.description AS description
                FROM    TreCode tre1
                INNER JOIN TreCode tre2
                ON      tre1.code = tre2.code
                WHERE   tre1.codetype = 4 
                AND     tre2.codetype = 5
               """
        result = self.query(query)
        result['description'] = result['description'].apply(
            lambda x: clean_string(x))
        return result

    def lookup_ibes_metric_by_name(self, metric_name):
        metrics = self.lookup_ibes_metrics()
        metric_code = metrics.loc[metrics.description ==
                                  metric_name, 'code'].values[0]
        return metric_code

    def get_ibes_key(self, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        # for the mapping to be logical, need to match on same typ
        query="""
        SELECT 
            CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			        ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			        ELSE mastX.prevSedol END
		        END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name,
            entity.Rank, -- Integer listing quotes, each new quote is one higher
            entity.Exchange, -- 1 for US, 2 for Canada, 0 for RoW
            entity.EstPermID as ibes_key,
            entity.EffectiveDate, -- first date where EPS forecasts exist for a EstPermId
            entity.ExpireDate, -- Last valid date of EPS forecasts for a EstPermID
            q.IsPrimary -- 1= primary quote
                
            FROM	dbo.vw_SecurityMasterX  mastX
                LEFT JOIN 
            -- Want to join with PermSecMapX on the quote object p.EntType = 55
            -- but also for US and Canada on Instrument object p.EntType = 49
            -- Denote Exchange =1 for US and =2 for Canada, ROW=0 (but not needed)

            (
                SELECT p.RegCode
                ,p.SecCode        
                ,p.[Rank]
                ,CASE WHEN i.CtryPermID = 100319 THEN 1 WHEN i.CtryPermID = 100052 THEN 2 ELSE 0 END AS Exchange
                ,i.EstPermID
                ,i.InstrPermID
                ,i.QuotePermID
                ,COALESCE(DATEADD(mi,-(i.EffectiveOffset),i.EffectiveDate),'2079-12-31') AS EffectiveDate
                ,COALESCE(DATEADD(mi,-(i.ExpireOffset),i.[ExpireDate]),'2079-12-31') AS [ExpireDate]
                ,i.CtryPermID 
                FROM PermSecMapX p 
                    JOIN TREInfo i 
                        ON p.EntPermID =i.InstrPermID 
                        AND p.EntType = 49 
                    JOIN PermInstrInfo ii 
                        ON ii.InstrPermID = i.InstrPermID 
                    WHERE i.CtryPermID IN (100052,100319) -- Selecting only US (100052) and Canada (100319)
                UNION -- Join the above to a similiar global quote level query below  
                SELECT p.RegCode
                    ,p.SecCode
                    ,p.[Rank] 
                    ,CASE WHEN i.CtryPermID = 100319 THEN 1 WHEN i.CtryPermID = 100052 THEN 2 ELSE 0 END AS Exchange
                    ,i.EstPermID
                    ,i.InstrPermID
                    ,i.QuotePermID
                    ,COALESCE(DATEADD(mi,-(i.EffectiveOffset),i.EffectiveDate),'2079-12-31') AS EffectiveDate
                    ,COALESCE(DATEADD(mi,-(i.ExpireOffset),i.[ExpireDate]),'2079-12-31') AS [ExpireDate]
                    ,i.CtryPermID 
                FROM PermSecMapX p 
                    JOIN TREInfo i 
                        ON p.EntPermID = i.QuotePermID 
                        AND p.EntType = 55
                     ) entity   
                    ON mastX.seccode=entity.seccode
                    AND ((entity.regcode='1' and mastX.typ='1') OR (entity.regcode='0' and mastX.typ='6'))
                -- join on Quote information to ascertain if it is the primary quote!
                    LEFT JOIN PermQuoteRef q
                        ON     q.QuotePermID = entity.QuotePermID              
            
                    WHERE ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}))
                        OR (mastX.cusip IN ({cusips})OR mastX.prevcusip IN ({cusips})))
                        AND entity.EstPermID IS NOT NULL""".format(
                    cusips=",".join(["'{}'".format(i) for i in cusip]),
                    sedols=",".join(["'{}'".format(i) for i in sedol]))
        # sort query results by preference for matches
        result=self.query(query).sort_values([ 'security_key_abbrev','IsPrimary','Rank','Exchange'],ascending=
                [True,False,True,True])
        filled_permid=~result['ibes_key'].isna()
        result.loc[filled_permid,'ibes_key']=result.loc[filled_permid,'ibes_key'].astype(int).astype(str)
        return result

    def get_ibes_fp0_date(self,
                          ibes_keys,
                          period_type=4):
        if len(ibes_keys) > 1:
            ibes_keys = ",".join(["'{}'".format(ibes_key)
                                  for ibes_key in ibes_keys])
        else:
            ibes_keys = str(ibes_keys[0])

        query = f"""
                SELECT	EstPermID AS ibes_key,
                        EffectiveDate AS date,
                        PerEndDate AS ibes_period{period_type}_fp0
                FROM	dbo.TREPerAdvance
                WHERE	EstPermID in ({ibes_keys})
                AND		PerType = {period_type}
                """

        result = self.query(query)
        result[f'ibes_period{period_type}_fp0'] = pd.to_datetime(
            result[f'ibes_period{period_type}_fp0'])
        result['date'] = pd.to_datetime(result['date'])
        result['ibes_key'] = result['ibes_key'].astype('str')
        return result

    def get_ibes_fpn_date(self,
                          since,
                          ibes_keys,
                          period_type=4,
                          forecast_period=1):
        if len(ibes_keys) > 1:
            ibes_keys = ",".join(["'{}'".format(ibes_key)
                                  for ibes_key in ibes_keys])
        else:
            ibes_keys = str(ibes_keys[0])

        query = f"""    
                SELECT	EstPermID AS ibes_key,
                        ExpireDate AS expire_date,
                        PerEndDate AS period_end_date_fp{forecast_period}
                FROM    dbo.TREPerIndex        
                WHERE	EstPermID IN ({ibes_keys})
                AND		PerType = {period_type}
                """
        result = self.query(query)
        result['expire_date'] = pd.to_datetime(result['expire_date'])
        result[f'period_end_date_fp{forecast_period}'] = pd.to_datetime(
            result[f'period_end_date_fp{forecast_period}'])
        result['ibes_key'] = result['ibes_key'].astype('str')
        return result

    def get_ibes_forecasts(self,
                           since,
                           until,
                           metric_name,
                           ibes_keys,
                           period_type=4,
                           forecast_period=1):

        if len(ibes_keys) > 1:
            ibes_keys = ",".join(["'{}'".format(ibes_key)
                                  for ibes_key in ibes_keys])
        else:
            ibes_keys = str(ibes_keys[0])

        metric_code = self.lookup_ibes_metric_by_name(metric_name)

        query = f"""        
                SELECT	DISTINCT
                        EstPermID AS ibes_key,
                        EffectiveDate AS date,
                        PerEndDate AS period{period_type}_end_date_fp{forecast_period},
                        DefMeanEst*DefScale AS {metric_name},
                        DefCurrPermID
                FROM	dbo.TRESumPer
                WHERE	PerType = {period_type}
                AND		EstPermID IN ({ibes_keys})
                AND     measure = {metric_code}
                AND		PerEndDate > '{since}'
                AND		EffectiveDate < '{until}'
                AND     IsParent= '0';
                """

        result = self.query(query)
        result.loc[:, 'DefCurrPermID'] = result['DefCurrPermID'].astype(str)
        result['date'] = pd.to_datetime(result['date'])
        result[f'period{period_type}_end_date_fp{forecast_period}'] = pd.to_datetime(
            result[f'period{period_type}_end_date_fp{forecast_period}'])
        result['ibes_key'] = result['ibes_key'].astype('str')
        return result

    def get_ibes_actuals(self,
                         since,
                         until,
                         metric_name,
                         ibes_keys,
                         period_type=4):

        if len(ibes_keys) > 1:
            ibes_keys = ",".join(["'{}'".format(ibes_key)
                                  for ibes_key in ibes_keys])
        else:
            ibes_keys = str(ibes_keys[0])

        metric_code = self.lookup_ibes_metric_by_name(metric_name)

        query = f"""        
                SELECT	DISTINCT
                        EstPermID AS ibes_key,
                        EffectiveDate AS date,
                        PerEndDate AS period_end_date_act,
                        DefActValue*DefScale AS {metric_name},
                        DefCurrPermID
                FROM	dbo.TREActRpt
                WHERE	PerType = {period_type}
                AND		EstPermID IN ({ibes_keys})
                AND     measure = {metric_code}
                AND		EffectiveDate < '{until}'
                AND     IsParent= '0';
                """

        result = self.query(query)
        result.loc[:, 'DefCurrPermID'] = result['DefCurrPermID'].astype(str)
        result['date'] = pd.to_datetime(result['date'])
        result[f'period_end_date_act'] = pd.to_datetime(
            result[f'period_end_date_act'])
        result['ibes_key'] = result['ibes_key'].astype('str')
        return result

    def ibes_currency_dictionary(self, codes):
        query = """SELECT m.Code   
                            ,m.Description as Currency
                            FROM 
                            dbo.TreCode m
                            where m.CodeType=7
                            and m.code in ({codes})
                            """.format(codes=",".join(["'{}'".format(i) for i in codes]))
        return self.query(query).set_index('Code')['Currency'].to_dict()

    def get_issuer_cusip8s_by_cusip8(self, cusip8s):
        query = """SELECT DISTINCT
                          a.cusip as cusip8, 
                          b.cusips as issuer_cusip8
                   FROM
                        (SELECT
                            issuer,
                            cusip
                        FROM
                            prc.PrcInfo) as a
                   INNER JOIN
                        (SELECT
                            issuer,
                            cusip as cusips
                        FROM
                            prc.PrcInfo) as b
                   on a.issuer = b.issuer
                   WHERE a.cusip in ({cusips})
        """.format(cusips=",".join(["'{}'".format(i) for i in cusip8s]))
        result = self.query(query)
        return result

    def get_ric_by_seccode(self, seccode):
        query = """SELECT
                   RDCRICData.ric as ric,
                   RDCRICData.StartDate as ric_StartDate,
                   RDCRICData.EndDate as ric_EndDate,
                   RDCSecMapX.seccode as seccode
                   FROM
                   RDCSecMapX
                   INNER JOIN
                   RDCRICData
                   ON RDCRICData.QuoteID = RDCSecMapX.VenCode
                   WHERE
                   RDCSecMapX.VenType = 55
                   AND RDCSecMapX.Exchange = 1
                   AND RDCSecMapX.seccode in ({seccode})
        """.format(seccode=",".join(["'{}'".format(int(i)) for i in seccode]))
        result = self.query(query)
        return result

    def get_gvkey_to_gic_since_until(self, gvkeys, since, until):
        query = """
                   SELECT
                          StartDate as since,
                          EndDate as until,
                          GvKey as gvkey,
                          GSubInd as gic
                   FROM
                          SPG2HGICS
                   WHERE --StartDate >= '{since}'
                         --AND EndDate <= '{until}'
                         --AND
                         GvKey in ({gvkeys})
                """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD),
                           gvkeys=",".join(["'{}'".format(i) for i in gvkeys]))
        result = self.query(query)
        result['gvkey'] = result.gvkey.apply(lambda x: str(x) if len(
            str(x)) == 6 else '0' * (6 - len(str(x))) + str(x))
        return result

    def get_sector_by_gic(self):
        query = f"""
        SELECT DISTINCT(g.SubindustryCode) AS gic,
            g.Sector as sector,
            g.IndustryGroup as industry_group,
            g.Industry as industry,
            g.SubIndustry as sub_industry
        FROM vw_SPGICSD g
        """
        result = self.query(query)
        result['gic'] = result.gic.astype(str)
        return result

    def get_sp_500_market_weights(self, since, until, cusips):
        query = """
                SELECT 
                    S.Cusip as security_key_abbrev,
                    'cusip' as security_key_name,
                    N.Date_ as date,
                    N."Weight" / 100 as sp_500_market_weight
                FROM 
                    qai.dbo.IdxSpCmp N
                JOIN 
                    qai.PRC.IDXSEC S
                    ON S.code = N.SecCode
                       AND N.IdxCode = 203
                       AND S.vendor = 1
                WHERE
                    S.Cusip in ({cusips})
                    AND N.Date_ >= '{since}'
                    AND N.Date_ <= '{until}'
                """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD),
                           cusips=",".join(["'{}'".format(i) for i in cusips]))
        return self.query(query)

    def get_sp_1500_market_weights(self, since, until, cusips):
        query = """
                SELECT 
                    S.Cusip as security_key_abbrev,
                    'cusip' as security_key_name,
                    N.Date_ as date,
                    N."Weight" / 100 as sp_1500_market_weight
                FROM 
                    qai.dbo.IdxSpCmp N
                JOIN 
                    qai.PRC.IDXSEC S
                    ON S.code = N.SecCode
                       AND N.IdxCode = 555
                       AND S.vendor = 1
                WHERE
                    S.Cusip in ({cusips})
                    AND N.Date_ >= '{since}'
                    AND N.Date_ <= '{until}'
                """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD),
                           cusips=",".join(["'{}'".format(i) for i in cusips]))
        return self.query(query)

    def get_merger_target_announcement_dates(self, since, until, cusip=[], sedol=[]):
        query = """
                SELECT
                CASE 
                    WHEN org.Cusip IS NOT NULL THEN org.Cusip
                    WHEN org.Sedol IS NOT NULL THEN org.Sedol
                    ELSE NULL
                END as security_key_temp,
                CASE 
                    WHEN org.Cusip IS NOT NULL THEN 'cusip'
                    WHEN org.Sedol IS NOT NULL THEN 'sedol'
                    ELSE NULL
                END as security_key_name,
                --players.OrgID,
                --players.TransactionId, 
                players.AnnDate as merger_target_announce_date
                --org.Cusip, 
                --org.Sedol, 
                --org.TickSym,
                --org.FullName
                FROM 
                    DLPriPlyrs players
                    INNER JOIN DLOrgInfo org 
                        ON org.OrgId = players.OrgId 
                        AND org.CIDGen = players.CIDGen
                    INNER JOIN DLTransInfo info
                        ON info.TransactionId = players.TransactionId
                    INNER JOIN DLDesc code
                        ON code.Code = info.SubStatusCode
                WHERE 
                    players.PrimRoleCode = 2 AND -- 2 Targets. acquirer is 1
                    players.AssetClassCode = 1 AND -- M&A
                    code.Type_ = 22 AND
                    code.Desc_ in ('Completed', 'Unconditional', 'Intended', 'Pending', 'Withdrawn') AND
                    info.MATypeCode in ('DI', 'UN')
        """
        if len(cusip) > 0 and len(sedol) > 0:
            query += """
                     AND  (org.Cusip in ({cusips}) OR org.Sedol in ({sedols}))
                     """.format(cusips=",".join(["'{}'".format(i[:6]) for i in cusip]),
                                sedols=",".join(["'{}'".format(i[:6])
                                                 for i in sedol]),
                                )
        elif len(cusip) > 0:
            query += """
                     AND  org.Cusip in ({cusips})
                     """.format(cusips=",".join(["'{}'".format(i[:6]) for i in cusip]),
                                )
        elif len(sedol) > 0:
            query += """
                      AND  org.Sedol in ({sedols})
                      """.format(sedols=",".join(["'{}'".format(i[:6]) for i in sedol]),
                                 )
        feature = self.query(query)
        lookup = {c[:6]: c for c in cusip}
        feature['merger_target_announce_date'] = pd.to_datetime(
            feature['merger_target_announce_date'])
        feature.loc[feature['security_key_name'] == 'cusip',
                    'security_key_abbrev'] = feature.loc[feature['security_key_name'] == 'cusip', 'security_key_temp'].map(lookup)
        return feature

    def get_vendor_code(self, ventype, cusip=[], sedol=[]):
        sedols = ["''" if len(sedol) == 0 else ",".join(
            ["'{}'".format(i) for i in sedol])][0]
        cusips = ["''" if len(cusip) == 0 else ",".join(
            ["'{}'".format(i) for i in cusip])][0]
        query = f"""
                SELECT
                    CASE
                        WHEN mastX.cusip IS NOT NULL THEN
                        CASE
                            WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
                        ELSE mastX.prevcusip END
                    ELSE
                        CASE
                            WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
                        ELSE mastX.prevSedol END
                    END AS security_key_abbrev,
                        MapX.VenCode as ven_code
                FROM vw_SecurityMasterX mastX
                INNER JOIN vw_SecurityMappingX MapX
                    ON mastX.seccode = MapX.seccode
                    AND mastX.typ = MapX.typ
                WHERE MapX.ventype={str(ventype)}
                    AND MapX.Rank=1
                    AND ((mastX.sedol IN ({sedols})
                        OR mastX.prevsedol IN ({sedols}))
                        OR (mastX.cusip IN ({cusips})
                            OR mastX.prevcusip IN ({cusips})))

                """
        return self.query(query)

    def get_vendor_type_name(self, ventype):
        """for a ventype used in the vw_SecurityMappingX view, return the name of the vendor"""
        query = f"""SELECT VenName
                FROM SecVenType
                WHERE VenType ={ventype}"""
        return self.query(query).iloc[0].VenName

    def get_last_fiscal_end_dates(self, df, period):
        worldscope_company_mapping_col = 'Worldscope Company Mapping'
        codes = ",".join(["'{}'".format(i) for i in df[~df[worldscope_company_mapping_col].isna(
        )][worldscope_company_mapping_col].unique()])
        query = f"""
                SELECT
                       Date_ as end_date,
                       Value_ as report_date,
                       seq as quarter,
                       code 
                FROM dbo.Wsddata
                WHERE code in ({codes})
                    AND freq='{period}'
                    AND Item='5905'
                ORDER BY Year_ DESC"""
        return self.query(query)

    def datastream_index_constituents(self, since, until, index_code):
        """ return the index constituents for an index in the datastream monthly index table"""
        query = """select i.StartDate as in_index_since, 
            ISNULL(i.EndDate, '{until}') as in_index_until,
            i.infocode
            from Ds2ConstMth i
            where i.indexlistintcode='{index_code}'
            AND ISNULL(i.EndDate, '{until}') >= '{since}'
            """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                       until=until.strftime(DATE_STRING_FORMAT_QAD),
                       index_code=index_code)
        result = self.query(query)
        if not result[result.infocode.isna()].empty:
            logging.warning(
                'missing index infocodes - {}'.format(result[result.infocode.isna()]))
            result = result[~result.infocode.isna()]
            result.loc[:, 'infocode'] = result.loc[:, 'infocode'].astype(int)
        result['in_index_since'] = pd.to_datetime(result['in_index_since'])
        result['in_index_until'] = pd.to_datetime(result['in_index_until'])
        return result
    
    def datastream_index_name_search(self, search_term=None):
        """Query the Datastream table listing indexes available to search for all matching indexes
        If no search term given, return whole table"""
        if search_term:
            query = f"""
            select
                IndexListIntCode as index_code,
                IndexListDesc as index_name,
                IndexListMnem as index_mnemonic
            FROM Ds2IndexList
                where IndexListDesc like '%{search_term}%'"""
        else:
            query = """
            select 
                IndexListIntCode as index_code,
                IndexListDesc as index_name,
                IndexListMnem as index_mnemonic
            FROM Ds2IndexList"""
        return self.query(query)

    def datastream_index_code_from_name(self, index_name):
        """given an DataStream index name, find the index numeric code """
        query = """select distinct IndexListIntCode from 
            Ds2IndexList
            where IndexListDesc = '{index_name}'""".format(index_name=index_name)
        result = self.query(query)
        if result.empty:
            return False
        else:
            return result.iloc[0]['IndexListIntCode']

    def get_security_keys_from_infocodes(self, infocodes):
        """Take the infocode (Datastream unique identifier) and return security_key_abbrev and security_key_name
        Uses the core QAD mapping and master tables"""
        query = """select CASE WHEN (mastX.cusip <>'None' and mastX.cusip is not null) THEN
			mastX.cusip 
		ELSE
			 mastX.Sedol END as security_key_abbrev,
        CASE WHEN (mastX.cusip <>'None' and mastX.cusip is not null) THEN
			'cusip'
		ELSE
			 'sedol' END as security_key_name,
		map.startdate,
		map.enddate,
		map.vencode as infocode
			from vw_securityMappingX map
        join vw_securityMasterX mastX
            on map.seccode=mastX.seccode 
            and map.typ=mastX.typ
        where map.vencode in ({infocode})
            and map.ventype='33'
            """.format(infocode=",".join(["'{}'".format(i) for i in infocodes]))

        result = self.query(query)
        result = result[~result.infocode.isna()]
        result.loc[:, 'infocode'] = result.loc[:, 'infocode'].astype(int)
        result['startdate'] = pd.to_datetime(result['startdate'])
        result['enddate'] = pd.to_datetime(result['enddate'])
        return result
    
    def get_security_keys_from_infocodes_now(self,infocodes):
        """Take the infocode (Datastream unique identifier) and return security_key_abbrev and security_key_name
        using the DataStream Mapping view"""
        query="""select D.vencode as infocode,
            CASE WHEN X.typ='1' THEN X.cusip 
                ELSE X.sedol END as security_key_abbrev,
            CASE WHEN X.typ='1' THEN 'cusip'
                ELSE 'sedol' END as security_key_name

            from dbo.vw_Ds2Mapping D
            join vw_securityMasterX X
            on D.seccode=X.seccode
              and D.typ=X.typ
            where vencode in ({infocode})
            """.format(infocode=",".join(["'{}'".format(i) for i in infocodes]))
        result = self.query(query)
        result = result[~result.infocode.isna()]
        result.loc[:, 'infocode'] = result.loc[:, 'infocode'].astype(int)
        return result

    def worldscope_item_name_dictionary(self):
        """query the worldscope tables to create a dictionary from item code to item name"""
        d = self.query("""select Number, Name from dbo.Wsitem
            """)
        d.set_index('Number', inplace=True)
        return d.to_dict()['Name']

    def get_worldscope_actuals(self, period, metric_code, worldscope_keys):
        """Query the vw_WSItemData view, which hosts fundamental data. Grab the EPSReportDate and currency if relevant
        Note worldscope_keys here can be company level or security level depending on the Item."""
        query = """
            SELECT
                f.code as worldscope_key, 
                f.fiscalPeriodEndDate fiscal_period_end_date,
                f.epsReportDate as date,
                f.Value_,
                f.itemUnits
            from dbo.vw_WSItemData f
                WHERE code in ({codes})
                AND freq='{period}'
                AND Item='{item}'
            ORDER BY worldscope_key,epsReportDate DESC,fiscalPeriodEndDate DESC
            """.format(period=period,
                       item=metric_code,
                       codes=",".join(["'{}'".format(i) for i in worldscope_keys]
                                      ))
        result = self.query(query)
        result['worldscope_key'] = result['worldscope_key'].astype(str)

        # want to keep itemUnits if either a 3 digit isocurr3ency code of isocurrency/share
        result['itemUnits'] = np.where(result.itemUnits.str.contains("/share"),
                                       result['itemUnits'].str.split(pat='/share', n=1, expand=True)[0],
                                       result['itemUnits'])

        result['worldscope_currency'] = np.where(result['itemUnits'].str.len() == 3,
                                                 result['itemUnits'],
                                                 np.NAN)

        metric_name = clean_string(
            self.worldscope_item_name_dictionary()[metric_code])
        result.rename(columns={'Value_': metric_name}, inplace=True)
        return result

    def worldscope_add_last_actual(self, worldscope_keys, period='A', metric_code=None, exact_match_allowed=True,
                                   keep_period_end_date=False, add_period_to_column_name=False):
        """ Adds in the last reported actual for the period type and metric provided.
        Provide either metric name or integer code as per  qad.worldscope_item_name_dictionary()
        :list(int) worldscope_keys: security/company keys for worldscope tables
        str period type of period, either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        int metric_code: code of metric to be added
        bool exact_match_allowed: Allow same day merges, if False joins with previous dat
        bool keep_period_end_date: Add a column denoting the period end date
        bool add_period_to_column_name: add'last_annual'/'last_quarter' to column names to avoid confusion

        """
        metric_name = clean_string(
            self.worldscope_item_name_dictionary()[metric_code])
        feature = self.get_worldscope_actuals(
            period, metric_code, worldscope_keys)
        if feature.empty:
            return pd.DataFrame()
        else:
            feature = convert_metric_to_currency_aware_column(feature,
                                                              metric_name,
                                                              'worldscope_currency')
            feature['date'] = pd.to_datetime(feature['date'])

            if add_period_to_column_name:
                if period in ["E", "Q", "H", "I", "R", "@"]:
                    feature.rename(
                        columns={metric_name: metric_name+"_last_quarter"}, inplace=True)
                    metric_name = metric_name+"_last_quarter"
                else:
                    feature.rename(
                        columns={metric_name: metric_name+"_last_annual"}, inplace=True)
                    metric_name = metric_name+"_last_annual"
            keep = ['date', 'worldscope_key', metric_name]
            if keep_period_end_date:
                if period in ["E", "Q", "H", "I", "R", "@"]:
                    feature.rename(columns={
                                   'fiscal_period_end_date': 'last_reported_quarter_end_date'}, inplace=True)
                    keep.append('last_reported_quarter_end_date')
                else:
                    feature.rename(
                        columns={'fiscal_period_end_date': 'last_reported_annual_end_date'}, inplace=True)
                    keep.append('last_reported_annual_end_date')

            return feature[~feature.date.isna()][keep]

    def worldscope_security_key(self,panel):
        """
        Adds Worldscope security Mapping column as worldscope_key. Warning many worldscope values are not filled at the security level 
        """
        panel = panel.features.vendor_code( 
            ventype=10, rename_column=True)
        
        return panel.rename(columns={'Worldscope Company Mapping': 'worldscope_security_key'})

    def worldscope_key(self,panel):
        """
        Adds Worldscope Company Mapping column as worldscope_key
        """
        keys = panel.features._get_keys(abbreviated=True)
        feature = self.get_worldscope_company_code(**keys)
        feature.loc[:,'worldscope_key'] = feature.loc[:,'worldscope_key'].astype(str)
        return  pd.merge(panel, feature, on='security_key_abbrev', how='left')

    def get_worldscope_company_code(self, cusip=[], sedol=[]):
        """SQL query to map from sedol or cusip to Worldscope Company identifier, rather than security identifier"""
        sedols = ["''" if len(sedol) == 0 else ",".join(["'{}'".format(i) for i in sedol])][0]
        cusips = ["''" if len(cusip) == 0 else ",".join(["'{}'".format(i) for i in cusip])][0]
        query = f"""        
            SELECT
                    CASE
                        WHEN mastX.cusip IS NOT NULL THEN
                        CASE
                            WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
                        ELSE mastX.prevcusip END
                    ELSE
                        CASE
                            WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
                        ELSE mastX.prevSedol END
                    END AS security_key_abbrev,
                        cmap.vencode as worldscope_key
                FROM vw_SecurityMasterX mastX
                INNER JOIN vw_WsCompanyMapping cmap
                    ON mastX.seccode = cmap.seccode
                    AND mastX.typ = cmap.typ
                WHERE ((mastX.sedol IN ({sedols})
                        OR mastX.prevsedol IN ({sedols}))
                        OR (mastX.cusip IN ({cusips})
                            OR mastX.prevcusip IN ({cusips})))

                """
        return self.query(query)

    def worldscope_industrial_classification(self, worldscope_keys):
        """retrieve the worldscope industrial classification for a list of worldscope_keys
        Certain worldscope metrics are only available for set industries (i.e cash 2003/2004)"""
        
        query="""
            select 
                H.Desc_ as worldscope_industrial_classification,
                I.Code as worldscope_key
            FROM Wsidata I
                join  WSCode H	
			        on I.Item=H.field
			        and I.Value_=H.Value_
                where I.Code in ({codes})
                    and I.Item='6010'""".format(codes=",".join(["'{}'".format(i) for i in worldscope_keys]))
        result=self.query(query)
        result.loc[:,'worldscope_key'] = result.loc[:,'worldscope_key'].astype(str)
        return result

    def cash_and_equivalents(self,df, period='A', exact_match_allowed=True, convert_currency=True):
        """ Function returns cash and cash equivalents on balance sheet in last published set of reports
        Includes short term investments 
        :str period: period type (NOT pandas standard) to retrive cash from, 
            either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :bool exact_match_allowed: Allow same day merges, if False joins with previous date
        :bool convert_currency: Convert all values to USD"""
        
        if "worldscope_key" not in df.columns:
            df = self.worldscope_key(df)
        worldscope_keys = df[~df.worldscope_key.isna()].worldscope_key.unique()
        feature = self.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=2005, exact_match_allowed=exact_match_allowed)
        feature.rename(columns={'cash___generic':"cash_and_equivalents"},inplace=True)
        df.loc[df.worldscope_key.isna(), 'worldscope_key'] = -99

        df = pd.merge_asof(df.sort_values(['date']),
                feature.sort_values(['date']),
                on='date', by='worldscope_key',allow_exact_matches=exact_match_allowed)
        df.drop(columns=['worldscope_key'], inplace=True)

        if convert_currency:
            df = df.units.convert_currency_aware_column(
                metric='cash_and_equivalents',  exact_day_match=exact_match_allowed)
        return df
    
    def get_sp_1200_membership(self,since, until):
        """this is the S&P global 1200 index, which is the S&P 500, canadian 60 and global 540 (ish)
        with gvkeyx '000003','118341' &'150918' respectively.
        Current and past consituents are in different tables, so need to separately query for each.
        Note that compustat security tables have cusip and sedol but we default to instead using
        the core QAD table mapping in vw_securityMappingX because it has validitiy dates for the mapping"""
        # first look for US and Canadian index members
        NA_query_current = """ 
            SELECT
                B.FROM_ as in_index_since,
                ISNULL(B.THRU, '{until}') as in_index_until,
                B.iid as iid,
                B.gvkey as gvkey,
                B.gvkeyx
            from dbo.CSIdxCstHis B		
                where 
                    B.GVKEYX in ('000003','118341')
                    AND ISNULL(B.THRU, '{until}') >= '{since}'
            """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD))
        NA_query_history = """ 
            SELECT
                B.FROM_ as in_index_since,
                ISNULL(B.THRU, '{until}') as in_index_until,
                B.iid as iid,
                B.gvkey as gvkey,
                B.gvkeyx
            from dbo.CSIdxCstHisSnP B		
                where 
                    B.GVKEYX in ('000003','118341')
                    AND ISNULL(B.THRU, '{until}') >= '{since}'
            """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD))
        #then Rest of World
        ROW_query_current="""
            SELECT
                B.FROM_ as in_index_since,
                ISNULL(B.THRU, '{until}') as in_index_until,
                B.iid as iid,
                B.gvkey as gvkey,
                B.gvkeyx
            from CSGIdxCstHis B 
                where 
                    B.GVKEYX in ('150918')
                    AND ISNULL(B.THRU, '{until}') >= '{since}'
            """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD))
        ROW_query_history="""
            SELECT
                B.FROM_ as in_index_since,
                ISNULL(B.THRU, '{until}') as in_index_until,
                B.iid as iid,
                B.gvkey as gvkey,
                B.gvkeyx
            from CSGIdxCstHisSnP B
                where 
                    B.GVKEYX in ('150918')
                    AND ISNULL(B.THRU, '{until}') >= '{since}'
            """.format(since=since.strftime(DATE_STRING_FORMAT_QAD),
                           until=until.strftime(DATE_STRING_FORMAT_QAD))
        #concat the four queries together
        result = pd.concat([self.query(NA_query_history),self.query(NA_query_current),
            self.query(ROW_query_history),self.query(ROW_query_current)])
        result['gvkey'] = result.gvkey.astype(str).str.zfill(6)
        result['gvkeyx'] = result.gvkeyx.astype(str).str.zfill(6)
        result['in_index_since'] = pd.to_datetime(result['in_index_since'])
        result['in_index_until'] = pd.to_datetime(result['in_index_until'])
        return result

    def security_name_QAD_master_table(self, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT distinct 
            name as issuer_name,
		    CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			    ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			    ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name
        from vw_SecurityMasterX mastX
            WHERE    ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}) )
            OR (mastX.cusip IN ({cusips}) OR mastX.prevcusip IN ({cusips}))
                )""".format(cusips=",".join(["'{}'".format(i) for i in cusip]),
                            sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def security_ISIN_QAD_master_table(self, cusip, sedol):
        if len(sedol) == 0:
            sedol = ['']
        if len(cusip) == 0:
            cusip = ['']
        query = """
        SELECT distinct 
            isin as issuer_ISIN,
		    CASE WHEN mastX.cusip IS NOT NULL THEN
			    CASE
				    WHEN mastX.cusip IN({cusips}) THEN mastX.cusip
			    ELSE mastX.prevcusip END 
		    ELSE
			    CASE
				    WHEN mastX.sedol IN({sedols}) THEN mastX.sedol
			    ELSE mastX.prevSedol END
		    END AS security_key_abbrev,
		    CASE 
                WHEN mastX.cusip IS NOT NULL THEN 'cusip' 
                ELSE 'sedol'
            END AS security_key_name
        
        from vw_SecurityMasterX mastX

        WHERE    ((mastX.sedol IN ({sedols}) OR mastX.prevsedol IN ({sedols}) )
            OR (mastX.cusip IN ({cusips}) OR mastX.prevcusip IN ({cusips}))
                )""".format(cusips=",".join(["'{}'".format(i) for i in cusip]),
                            sedols=",".join(["'{}'".format(i) for i in sedol]))
        return self.query(query)

    def get_worldscope_feature(self, panel, feature_name, feature_code, period='A',
                               exact_match_allowed=True, convert_currency=True,
                               is_security_level=False, db_column_name=None):
        """
        Helper function to add worldscope feature to a minimal copy of the panel.
        The caller function to this function is the user facing function which dictates
        which worldscope feature has to be added.

        Company refers to the whole organisation.
        Security refers to the specific assets, earnings and returns due to each instrument/listing/share class.

        Some examples for company level metrics are EBITDA, Enterprise Value, Sales
        Some examples for security level metrics are Earnings Per Share, Book Value Per Share

        :param panel: Pandas Dataframe
                    Panel object conforming to panel tool object standards
        :param feature_name: str
                    Determines the column name of the added feature
        :param feature_code: int
                    Worldscope item code of feature to be added
        :param period: str, default 'A'
                    period type (NOT pandas standard) to retrive cash from,
                    either annual type ['A','B','G'] or quarterly type ["E","Q","H","I","R","@"]
        :param exact_match_allowed: bool, default True
                    - If True, allow matching with the same 'on' value
                      (i.e. less-than-or-equal-to / greater-than-or-equal-to)
                    - If False, don't match the same 'on' value
                      (i.e., strictly less-than / strictly greater-than).
        :param convert_currency: bool, default True
                            - If True, convert added feature's currency to `to_currency`
                            - If False, keep as a currency aware object (XMoney)
        :param is_security_level: bool, default False
                            - If True, uses security level keys to join the feature on the panel
                            - If False, uses company level keys to join the feature on the panel
        :param db_column_name: str, default None
                            - If given, replaces the column name with this string with the `feature_name`
        :returns: A minimal copy of the panel with ['security_key_name','security_key', 'date', `feature_name`]
        """
        df = panel[panel.features.unit_key + panel.features.time_key].copy()
        if is_security_level:
            worldscope_key = 'worldscope_security_key'
            df = self.worldscope_security_key(df)
        else:
            worldscope_key = 'worldscope_key'
            df = self.worldscope_key(df)

        df.loc[df[worldscope_key].isna(), worldscope_key] = "-99"

        worldscope_keys = df[~df[worldscope_key].isna()][worldscope_key].unique()

        feature = self.worldscope_add_last_actual(
            worldscope_keys, period=period, metric_code=feature_code, exact_match_allowed=exact_match_allowed)
        feature = feature.rename(columns={'worldscope_key': worldscope_key})

        if db_column_name is not None:
            feature.rename(columns={db_column_name: feature_name}, inplace=True)

        df = df.features._asof_merge_feature(feature,
                                             feature_name,
                                             on=df.features.time_key,
                                             by=[worldscope_key],
                                             exact_match_allowed=exact_match_allowed)

        if convert_currency:
            df = df.units.convert_currency_aware_column(
                metric=feature_name, exact_day_match=True)

        return df.drop(columns=['security_key_abbrev', worldscope_key])

