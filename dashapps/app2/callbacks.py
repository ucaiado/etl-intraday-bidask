from datetime import datetime as dt

import pandas_datareader as pdr
from dash.dependencies import Input
from dash.dependencies import Output
import pandas as pd
from pyathena import connect
import configparser


def register_callbacks(dashapp):
    @dashapp.callback(
        Output('my-graph', 'figure'),
        [Input("input1", "value"),
         Input("input2", "date"),
         Input("refresh-button", "n_clicks")])
    def update_graph(input1, input2, n):
        # define S3 bucket and connect to Athena
        config = configparser.ConfigParser()
        config.read_file(open('confs/dpipe.cfg'))
        DL_DATA_BUCKET_NAME = config.get("CLUSTER", "DL_DATA_BUCKET_NAME")

        s3_bucket = f"s3://{DL_DATA_BUCKET_NAME}/quotes.parquet/"
        conn = connect(s3_staging_dir=s3_bucket, region_name='us-west-2')

        # retrieve and reshape data
        s_input = input1.upper()
        s_year, s_month, s_day = input2[:10].split('-')
        s_sql = f"""
            SELECT intdate, ticker, bid, ask, qty_bid, qty_ask
              FROM processed.quotes
              WHERE (
                  ticker='{s_input}'
                  AND year>={int(s_year)}
                  AND month>={int(s_month)}
                  AND day>={int(s_day)}
              )
              ORDER BY intdate ASC;
            """
        df = pd.read_sql(s_sql, conn)

        df = df.groupby(['intdate', 'ticker']).last().unstack().dropna()
        df.index = pd.to_datetime(df.index, format='%Y%m%d%H%M')

        df_micro_price = df['qty_bid'] * df['ask']
        df_micro_price += df['qty_ask'] * df['bid']
        df_micro_price /= (df['qty_bid'] + df['qty_ask'])
        df_micro_price = df_micro_price.round(3)
        df_micro_price.columns = ['micro']

        # return data to plot
        if df_micro_price.shape[0] == 0:
            return {
                'data': [{
                    'x': [],
                    'y': []
                }],
                'layout': {
                    'title': {'text': s_input},
                    'margin': {'l': 40, 'r': 0, 't': 50, 'b': 30},
                    'xaxis': dict(
                        rangebreaks=[
                            dict(bounds=["sat", "mon"]),
                            dict(pattern='hour', bounds=[17, 11])
                        ])
                    }
                }

        return {
            'data': [{
                'x': df_micro_price.index,
                'y': df_micro_price['micro']
            }],
            'layout': {
                'title': {'text': s_input},
                'margin': {'l': 40, 'r': 0, 't': 50, 'b': 30},
                'xaxis': dict(
                    rangebreaks=[
                        dict(bounds=["sat", "mon"]),
                        dict(pattern='hour', bounds=[17, 11])
                    ])
            }
        }
