#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Process data from files stored in S3 bucket and save the data transformed back
to another S3 bucket

@author: udacity, ucaiado

Created on 06/29/2020
"""

import argparse
import textwrap
import time
import os
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (udf, col, row_number, regexp_replace,
                                   input_file_name)
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)
from pyspark.sql.types import (LongType, IntegerType, DateType, StringType,
                               FloatType, StructType, StructField)


'''
Begin help functions and variables
'''


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def convert_str2float(x):
    # if isinstance(x, str):
    if isinstance(x, type(None)):
        return np.nan
    if x == 'Abe':
        return np.nan
    if 'm' in x:
        l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('m')]
        if len(l_nm) > 1:
            return float(l_nm[0] * 10**5 + l_nm[1] * 1000)
        return float(l_nm[0] * 10**5)
    elif 'k' in x:
        l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('k')]
        if len(l_nm) > 1:
            return float(l_nm[0] * 10**3 + l_nm[1])
        return float(l_nm[0] * 10**3)
    elif 'b' in x:
        l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('b')]
        if len(l_nm) > 1:
            return float(l_nm[0] * 10**7 + l_nm[1] * 10 ** 6)
        return float(l_nm[0] * 10**7)
    return float(x.replace(',', '.'))
    # return float(x)


def convert_str2int(x):
    # if isinstance(x, str):
    if isinstance(x, type(None)):
        return np.nan
    if 'm' in x:
        l_nm = [int(xx + '0' * (3-len(xx))) for xx in x.split('m')]
        if len(l_nm) > 1:
            return l_nm[0] * 10**5 + l_nm[1] * 1000
        return l_nm[0] * 10**5
    elif 'k' in x:
        l_nm = [int(xx + '0' * (3-len(xx))) for xx in x.split('k')]
        if len(l_nm) > 1:
            return l_nm[0] * 10**3 + l_nm[1]
        return l_nm[0] * 10**3
    elif 'b' in x:
        l_nm = [int(xx + '0' * (3-len(xx))) for xx in x.split('b')]
        if len(l_nm) > 1:
            return l_nm[0] * 10**7 + l_nm[1] * 10 ** 6
        return l_nm[0] * 10**7
    return int(x.replace('.', ''))
    # return x


def load_quotes(spark, s_path2s3):

    # define schema
    print('loading {}...'.format(s_path2s3))
    this_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("open", StringType(), True),
        StructField("settle", StringType(), True),
        StructField("lclose", StringType(), True),
        StructField("max", StringType(), True),
        StructField("min", StringType(), True),
        StructField("bid", StringType(), True),
        StructField("ask", StringType(), True),
        StructField("qbid", StringType(), True),
        StructField("qask", StringType(), True),
        StructField("qlast", StringType(), True),
        StructField("last", StringType(), True),
        StructField("group", StringType(), True),
        StructField("ntrades", StringType(), True),
        StructField("name", StringType(), True),
        StructField("qtotal", StringType(), True),
        StructField("strike", StringType(), True),
        StructField("expdate", StringType(), True),
        StructField("totalvol", StringType(), True)])

    # read csv
    df = (spark.read
          .option("header", "true")
          .schema(this_schema)
          .csv(s_path2s3)
          .withColumn("input_file", input_file_name()))

    # drop rows where ticker is null
    df = df.na.drop(subset=["ticker"])
    return df


def load_options(spark, s_path2s3):

    # define schema
    print('loading {}...'.format(s_path2s3))
    this_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("Du", StringType(), True),
        StructField("ExrcPric", StringType(), True),
        StructField("OptnStyle", StringType(), True),
        StructField("OptnTp", StringType(), True),
        StructField("TradgEndDt", StringType(), True),
        StructField("Underlying", StringType(), True),
        StructField("bid_opton", StringType(), True),
        StructField("bid_undly", StringType(), True),
        StructField("ask_opton", StringType(), True),
        StructField("ask_undly", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("bs_ask", StringType(), True),
        StructField("bs_bid", StringType(), True),
        StructField("my_ask", StringType(), True),
        StructField("my_bid", StringType(), True),
        StructField("bid_vol_imp", StringType(), True),
        StructField("ask_vol_imp", StringType(), True),
        StructField("bid_vol_cone", StringType(), True),
        StructField("ask_vol_cone", StringType(), True),
        StructField("bs_delta", StringType(), True),
        StructField("new_bid_vol_imp", StringType(), True),
        StructField("new_ask_vol_imp", StringType(), True)
    ])

    # read csv
    df = (spark.read
          .option("sep", ";")
          .option("header", "true")
          .schema(this_schema)
          .csv(s_path2s3)
          .withColumn("input_file", input_file_name()))

    # drop rows where ticker is null
    df = df.na.drop(subset=["ticker"])
    return df


'''
End help functions and variables
'''


def process_quotes_data(spark, input_data, output_data):
    '''
    Porcess data from quotes files

    :param spark: Spark session object.
    :param input_data: string. S3 path data to transform
    :param output_data: string. S3 root path to store transformed data
    '''

    # read log data file
    df = load_quotes(spark, input_data)

    # create intdate column to help filter and sort data in the future
    get_ints_from_file = udf(lambda x: int(
        str(x).split('/')[-1].split('_')[0]), LongType())
    df = df.withColumn('intdate', get_ints_from_file('input_file'))

    print(df.filter(df.ticker == 'PETR4')
            .select('intdate', 'ticker', 'bid', 'ask')
            .sort(df.intdate.asc())
            .show(5))

    # create datetime column
    get_datetime = udf(lambda x: datetime(
        *time.strptime(str(x), '%Y%m%d%H%S')[:6]), DateType())
    df = df.withColumn('datetime', get_datetime('intdate'))

    # convert strings to floats in finatial related fields
    get_str2float = udf(lambda x: convert_str2float(x), FloatType())
    for s_s2cast in ['bid', 'ask', 'open', 'settle', 'lclose', 'max', 'min',
                     'last', 'strike', 'totalvol']:
        # df = df.withColumn(f'{s_s2cast}2', get_str2float(f'{s_s2cast}'))
        df = df.withColumn(
            '{}2'.format(s_s2cast), get_str2float('{}'.format(s_s2cast)))

    # convert strings to integers in quantity related fields
    get_str2int = udf(lambda x: convert_str2int(x), IntegerType())
    for s_s2cast in ['qbid', 'qask', 'qlast', 'qtotal', 'ntrades']:
        # df = df.withColumn(f'{s_s2cast}2', get_str2int(f'{s_s2cast}'))
        df = df.withColumn(
            '{}2'.format(s_s2cast), get_str2int('{}'.format(s_s2cast)))

    print(df.filter(df.ticker == 'PETR4')
            .select('intdate', 'ticker', 'bid2', 'ask2', 'bid', 'ask')
            .sort(df.intdate.asc())
            .show(5))

    # extract columns to create quotes table
    quotes_table = df.select(
            col('intdate').alias('intdate'),
            year('datetime').alias('year'),
            month('datetime').alias('month'),
            dayofmonth('datetime').alias('day'),
            col('ticker').alias('ticker'),
            col('lclose2').alias('last_close'),
            col('open2').alias('open'),
            col('max2').alias('max'),
            col('min2').alias('min'),
            col('last2').alias('last'),
            col('qbid2').alias('qty_bid'),
            col('bid2').alias('bid'),
            col('ask2').alias('ask'),
            col('qask2').alias('qty_ask'),
            col('ntrades2').alias('num_trades'),
            col('qtotal2').alias('qty_total'),
            col('totalvol2').alias('total_volume'),
         )

    # write time table to parquet files partitioned by year and month
    s_s3_path = "{}/quotes.parquet".format(output_data)
    print('.. writing into {}'.format(s_s3_path))
    # NOTE: the magic to overwrite JUST partitions, not all the folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    quotes_table.write.partitionBy(
        'year', 'month', 'day').mode('overwrite').parquet(s_s3_path)

    # print out sample
    print(quotes_table.filter(quotes_table.ticker == "PETR4").show(5))
    print()
    print(df.filter(df.ticker == 'PETR4')
            .select('intdate', 'ticker', 'bid2', 'ask2', 'bid', 'ask')
            .sort(df.intdate.asc())
            .show(5))


def process_options_data(spark, input_data, output_data):
    '''
    Porcess data from quotes files

    :param spark: Spark session object.
    :param input_data: string. S3 path data to transform
    :param output_data: string. S3 root path to store transformed data
    '''

    # read log data file
    df = load_options(spark, input_data)

    # create intdate column to help filter and sort data in the future
    get_ints_from_file = udf(lambda x: int(
        str(x).split('/')[-1].split('_')[0]), LongType())
    df = df.withColumn('intdate', get_ints_from_file('input_file'))

    # create datetime column
    get_datetime = udf(lambda x: datetime(
        *time.strptime(str(x), '%Y%m%d%H%S')[:6]), DateType())
    df = df.withColumn('datetime', get_datetime('intdate'))

    # convert strings to floats in finatial related fields
    get_str2float = udf(lambda x: convert_str2float(x), FloatType())
    for s_s2cast in ['ExrcPric', 'bid_opton', 'bid_undly', 'ask_opton',
                     'ask_undly', 'bs_ask', 'bs_bid', 'my_ask', 'my_bid',
                     'bid_vol_imp', 'ask_vol_imp', 'bid_vol_cone',
                     'ask_vol_cone', 'bs_delta', 'new_bid_vol_imp',
                     'new_ask_vol_imp']:
        df = df.withColumn(
            '{}2'.format(s_s2cast), get_str2float('{}'.format(s_s2cast)))

    # convert strings to integers in quantity related fields
    get_str2int = udf(lambda x: convert_str2int(x), IntegerType())
    for s_s2cast in ['Du',  'sector']:
        df = df.withColumn(
            '{}2'.format(s_s2cast), get_str2int('{}'.format(s_s2cast)))

    # extract columns to create quotes table
    options_table = df.select(
        col('intdate').alias('intdate'),
        year('datetime').alias('year'),
        month('datetime').alias('month'),
        dayofmonth('datetime').alias('day'),
        col('ticker').alias('ticker'),
        col('Underlying').alias('optn_ua'),  # underlying asset
        col('OptnStyle').alias('optn_style'),
        col('OptnTp').alias('optn_tp'),
        col('DU2').alias('optn_expn_days'),
        col('TradgEndDt').alias('optn_expn_date'),
        col('ExrcPric2').alias('optn_strike'),
        col('bs_delta2').alias('optn_bs_delta'),
        col('bid_opton2').alias('optn_mkt_bid'),
        col('bs_bid2').alias('optn_bs_bid'),
        col('my_bid2').alias('optn_mktbs_bid'),
        col('bid_undly2').alias('ua_mkt_bid'),
        col('ask_opton2').alias('optn_mkt_ask'),
        col('bs_ask2').alias('optn_bs_ask'),
        col('my_ask2').alias('optn_mktbs_ask'),
        col('ask_undly2').alias('ua_mkt_ask'),
        col('bid_vol_imp2').alias('bid_volimp'),
        col('bid_vol_cone2').alias('bid_volcone'),
        col('ask_vol_imp2').alias('ask_volimp'),
        col('ask_vol_cone2').alias('ask_volcone'),
     )
    # write time table to parquet files partitioned by year and month
    s_s3_path = "{}/options.parquet".format(output_data)
    print('.. writing into {}'.format(s_s3_path))
    # NOTE: the magic to overwrite JUST partitions, not all the folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    options_table.write.mode('overwrite').partitionBy(
        'year', 'month', 'day').parquet(s_s3_path)

    # print out sample
    print(options_table.filter(options_table.optn_ua == "PETR4").show(5))

    return options_table


def main(s_input_quotes, s_input_options, s_output):
    '''
    Load data into stanging and analytical tables

    :param s_input_quotes: string. S3 path to quotes data to transform
    :param s_input_options: string. S3 path to options data to transform
    :param s_output: string. S3 root path to store transformed data
    '''

    spark = create_spark_session()
    input_quotes_data = s_input_quotes
    input_options_data = s_input_options
    output_data = s_output

    process_quotes_data(spark, input_quotes_data, output_data)
    process_options_data(spark, input_options_data, output_data)


if __name__ == "__main__":
    s_txt = '''\
            Extract, transform and load data to Data Lake in S3
            --------------------------------
            ..
            '''
    # include and parse variables
    obj_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=obj_formatter, description=textwrap.dedent(s_txt))

    s_hlp = 'input S3 path to quotes data'
    parser.add_argument('-q', '--quotes', default=None, type=str, help=s_hlp)

    s_hlp = 'input S3 path to options data'
    parser.add_argument('-op', '--options', default=None, type=str, help=s_hlp)

    s_hlp = 'date to filter files'
    parser.add_argument('-d', '--date', default=None, type=str, help=s_hlp)

    s_hlp = 'output S3 path'
    parser.add_argument('-o', '--output', default=None, type=str, help=s_hlp)

    # recover arguments
    args = parser.parse_args()
    s_input_quotes = 's3a://{}/{}*_flashpanel.csv'.format(
        args.quotes, args.date)
    s_input_options = 's3a://{}/{}*_options.csv'.format(
        args.options, args.date)
    s_output = 's3a://{}'.format(args.output)

    s_err = 'Enter an S3 path to both input and output files'
    assert s_output and s_input_quotes and s_input_options, s_err

    # process data
    print(s_input_quotes, s_input_options, s_output)
    main(s_input_quotes, s_input_options, s_output)
