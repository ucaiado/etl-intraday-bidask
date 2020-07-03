# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: zipline
#     language: python
#     name: zipline
# ---

# ## start Spark



from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ## Load market data

# +
from pyspark.sql.functions import regexp_replace, input_file_name
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, FloatType)

# sample: 202004011320,Abe,Ajuste,Fec,Max,Min,Ofc,Ofv,QOfc,QOfv,QUlt,Ult,Grupo,
#         NNeg,Nome,QTot,Strike,VCTO,VolTot
# NOTE: for some reason, I should first import the data THEN cast it to the
#  correct types

schema = StructType([
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
    StructField("qlast", IntegerType(), True),
    StructField("last", StringType(), True),
    StructField("group", StringType(), True),
    StructField("ntrades", StringType(), True),
    StructField("name", StringType(), True),
    StructField("qtotal", StringType(), True),
    StructField("strike", StringType(), True),
    StructField("expdate", StringType(), True),
    StructField("totalvol", StringType(), True)])

def load_data(s_path2s3, this_schema=schema):
    # read csv
    df = (spark.read
              .option("header", "true")
              .schema(this_schema)
              .csv(s_path2s3)
              .withColumn("input_file", input_file_name()))

    # drop rows where ticker is null
    df = df.na.drop(subset=["ticker"])
    return df


# -

# 202004011320_flashpanel.csv 
data_quotes = 's3a://data-from-quotes/20200411*_flashpanel.csv'
data_quotes = 's3a://data-from-quotes/*_flashpanel.csv'

df = spark.read.csv(data_quotes)
df.count()



df = load_data(data_quotes)
df.printSchema()

# ## Exploring the data

(df.filter(df.ticker == 'PETR4')
   .select('ticker', 'bid', 'ask')
   .show(5))

(df.filter(df.ticker == 'PETR4').count())

(df.filter(df.ticker == 'PETR4')
   .select('input_file')
   .show(5))



# ## Transforming the data

# +
# create timestamp column from original timestamp column

import time
import numpy as np
from datetime import datetime
from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType, IntegerType, DateType, StringType, FloatType


get_ints_from_file = udf(lambda x: int(str(x).split('/')[-1].split('_')[0]), LongType())
df2 = df.withColumn('intdate', get_ints_from_file('input_file'))


get_datetime = udf(lambda x: datetime(*time.strptime(str(x), '%Y%m%d%H%S')[:6]), DateType())
df2 = df2.withColumn('datetime', get_datetime('intdate'))


def convert_str2float(x):
    if isinstance(x, str):
        if x == 'Abe':
            return np.nan
        if 'm' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('m')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**5 + l_nm[1] * 1000
            return l_nm[0] * 10**5
        elif 'k' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('k')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**3 + l_nm[1]
            return l_nm[0] * 10**3
        elif 'b' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('b')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**7 + l_nm[1] * 10 ** 6
            return l_nm[0] * 10**7
        return float(x.replace(',', '.'))
    return x


# 28m938
# 223k
# 111k5
# 40m876
# 895k1

def convert_str2int(x):
    if isinstance(x, str):
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
    return x


get_str2float = udf(lambda x: convert_str2float(x), FloatType())
for s_s2cast in ['bid', 'ask', 'open', 'settle', 'lclose', 'max', 'min', 'last', 'strike', 'totalvol']:
    df2 = df2.withColumn(f'{s_s2cast}2', get_str2float(f'{s_s2cast}'))
    
get_str2int = udf(lambda x: convert_str2int(x), IntegerType())
for s_s2cast in ['qbid', 'qask', 'qlast', 'qtotal', 'ntrades']:
    df2 = df2.withColumn(f'{s_s2cast}2', get_str2int(f'{s_s2cast}'))

df2.printSchema()
# -

df2.select(['bid', 'ask', 'bid2', 'ask2', 'ticker']).limit(5).show(5, truncate=False)

# ## Checking functions

# +
x = '8,27'
print(x, convert_str2float(x), type(convert_str2float(x)))
x = '8.27'
print(x, convert_str2float(x), type(convert_str2float(x)))
x = 'Abe'
print(x, convert_str2float(x), type(convert_str2float(x)))
x = '350k4'
print(x, convert_str2int(x), type(convert_str2int(x)))
x = '223k'
print(x, convert_str2int(x), type(convert_str2int(x)))
x = '40m876'
print(x, convert_str2int(x), type(convert_str2int(x)))
x = '28m938'
print(x, convert_str2float(x), type(convert_str2float(x)))

x = '28m938'
print(x, convert_str2int(x), type(convert_str2int(x)))

x = '28m'
print(x, convert_str2int(x), type(convert_str2int(x)))

x = '28m'
print(x, convert_str2float(x), type(convert_str2float(x)))


x = '133k3'
print(x, convert_str2int(x), type(convert_str2int(x)))

x = '133k3'
print(x, convert_str2float(x), type(convert_str2float(x)))


x = '3b375'
print(x, convert_str2float(x), type(convert_str2float(x)))

x = '3b375'
print(x, convert_str2int(x), type(convert_str2int(x)))




# -



# ## Testing the transformations

(df2.filter(df2.ticker == 'B3SAR342')
   .select('datetime', 'intdate', 'ticker', 'bid2', 'ask2')
   .sort(df2.intdate.asc())
   .show(5))

(df2.filter(df2.ticker == 'PETR4')
   .select('datetime', 'intdate', 'ticker', 'bid2', 'ask2')
   .sort(df2.intdate.asc())
   .show(5))

# ## Create quotes fact table

# +
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)

quotes_table = df2.select(
        col('datetime').alias('date'),
        col('intdate').alias('intdate'),
        year('datetime').alias('year'),
        month('datetime').alias('month'),
        dayofmonth('datetime').alias('day'),
        col('ticker').alias('ticker'),
        col('lclose2').alias('last.close'),
        col('open2').alias('open'),
        col('max2').alias('max'),
        col('min2').alias('min'),
        col('last2').alias('last'),
        col('bid2').alias('bid'),
        col('ask2').alias('ask'),
        col('ntrades2').alias('num.trades'),
        col('qtotal2').alias('qty.total'),
        col('totalvol2').alias('total.volume'),
     )

quotes_table.printSchema()
# -

quotes_table.limit(5).show(5, truncate=False)

quotes_table.filter(quotes_table.ticker == "PETR4").show(5)

output_data = 'quotes-from-spark'
s_s3_path = f"s3a://{output_data}/quotes.parquet"
quotes_table.write.mode('overwrite').partitionBy('year', 'month', 'day').parquet(s_s3_path)



# ## Testing functions created to ETL script

from datetime import datetime
import time
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (udf, col, row_number, regexp_replace,
                                   input_file_name)
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)
from pyspark.sql.types import (LongType, IntegerType, DateType, StringType,
                               FloatType, StructType, StructField)


# +
def convert_str2float(x):
    if isinstance(x, str):
        if x == 'Abe':
            return np.nan
        if 'm' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('m')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**5 + l_nm[1] * 1000
            return l_nm[0] * 10**5
        elif 'k' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('k')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**3 + l_nm[1]
            return l_nm[0] * 10**3
        elif 'b' in x:
            l_nm = [float(xx + '0' * (3-len(xx))) for xx in x.split('b')]
            if len(l_nm) > 1:
                return l_nm[0] * 10**7 + l_nm[1] * 10 ** 6
            return l_nm[0] * 10**7
        return float(x.replace(',', '.'))
    return x


def convert_str2int(x):
    if isinstance(x, str):
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
    return x


# -

def load_data(spark, s_path2s3):

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
        StructField("qlast", IntegerType(), True),
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


# +
def process_quotes_data(spark, input_data, output_data):
    '''
    Porcess data from quotes files

    :param spark: Spark session object.
    :param input_data: string. S3 path data to transform
    :param output_data: string. S3 root path to store transformed data
    '''

    # read log data file
    df = load_data(spark, input_data)

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

    # extract columns to create quotes table
    quotes_table = df.select(
            col('intdate').alias('intdate'),
            year('datetime').alias('year'),
            month('datetime').alias('month'),
            dayofmonth('datetime').alias('day'),
            col('ticker').alias('ticker'),
            col('lclose2').alias('last.close'),
            col('open2').alias('open'),
            col('max2').alias('max'),
            col('min2').alias('min'),
            col('last2').alias('last'),
            col('bid2').alias('bid'),
            col('ask2').alias('ask'),
            col('ntrades2').alias('num.trades'),
            col('qtotal2').alias('qty.total'),
            col('totalvol2').alias('total.volume'),
         )
#     write time table to parquet files partitioned by year and month

#     spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#     data.toDF().write.mode("overwrite").format("parquet").partitionBy("date", "name").save("s3://path/to/somewhere")
    s_s3_path = "{}/quotes.parquet".format(output_data)
    print('.. writing into {}'.format(s_s3_path))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    quotes_table.write.mode('overwrite').partitionBy(
        'year', 'month', 'day').parquet(s_s3_path)
    
    return quotes_table


# +
# spark_etl.py -q data-from-quotes -op data-from-options -d 20200402 -o data-from-etl

class foo():
    quotes = 'data-from-quotes'
    options = 'data-from-options'
    output = 'data-from-etl'
    date = 20200402


# -



# +
args = foo()

s_input_quotes = 's3a://{}/{}*_flashpanel.csv'.format(
    args.quotes, args.date)
s_input_options = 's3a://{}/{}*_options.csv'.format(
    args.options, args.date)
s_output = 's3a://{}'.format(args.output)

s_err = 'Enter an S3 path to both input and output files'
assert s_output and s_input_quotes and s_input_options, s_err

# +
input_quotes_data = s_input_quotes
input_options_data = s_input_options
output_data = s_output

quotes_table = process_quotes_data(spark, input_quotes_data, output_data)
# -

quotes_table.filter(quotes_table.ticker == "PETR4").show(5)





# ## Load options data

# s3://data-from-options/202004011340_options.csv
data_options = 's3a://data-from-options/*_options.csv'
data_options = 's3a://data-from-options/20200401*_options.csv'

# +
# 2020-04-01 13:51:41.017496;Du;ExrcPric;OptnStyle;OptnTp;TradgEndDt;Underlying;bid_opton;
# bid_undly;ask_opton;ask_undly;sector;bs_ask;bs_bid;my_ask;my_bid;bid_vol_imp;ask_vol_imp;#
# bid_vol_cone;ask_vol_cone;bs_delta;new_bid_vol_imp;new_ask_vol_im
# -

df = spark.read.option("sep", ";").option("header", "true").csv(data_options)
df.printSchema()

df.count()

# +
from pyspark.sql.functions import regexp_replace, input_file_name
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, FloatType)

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


# -

df = load_options(spark, data_options)
df.printSchema()

df.count()

# ## Exploring the data

(df.filter(df.Underlying == 'B3SA3')
   .select('ticker', 'Du', 'bid_opton', 'ask_opton', 'bid_undly', 'ask_undly', 'sector')
   .show(5))

(df.filter(df.Underlying == 'PETR4')
   .select('input_file')
   .show(5))

df.schema.names

# ## Transforming the data and creating options fact table

# +

from datetime import datetime
import time
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (udf, col, row_number, regexp_replace,
                                   input_file_name)
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek)
from pyspark.sql.types import (LongType, IntegerType, DateType, StringType,
                               FloatType, StructType, StructField)

# -

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
    for s_s2cast in ['ExrcPric', 'bid_opton', 'bid_undly', 'ask_opton', 'ask_undly',
                     'bs_ask', 'bs_bid', 'my_ask', 'my_bid', 'bid_vol_imp', 'ask_vol_imp',
                     'bid_vol_cone', 'ask_vol_cone', 'bs_delta', 'new_bid_vol_imp',
                     'new_ask_vol_imp']:
        # df = df.withColumn(f'{s_s2cast}2', get_str2float(f'{s_s2cast}'))
        df = df.withColumn(
            '{}2'.format(s_s2cast), get_str2float('{}'.format(s_s2cast)))

    # convert strings to integers in quantity related fields
    get_str2int = udf(lambda x: convert_str2int(x), IntegerType())
    for s_s2cast in ['Du',  'sector']:
        # df = df.withColumn(f'{s_s2cast}2', get_str2int(f'{s_s2cast}'))
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
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    options_table.write.mode('overwrite').partitionBy(
        'year', 'month', 'day').parquet(s_s3_path)
    
    return options_table


class foo():
    quotes = 'data-from-quotes'
    options = 'data-from-options'
    output = 'data-from-etl'
    date = 20200402


s_input_quotes = 's3a://{}/{}*_flashpanel.csv'.format(
    args.quotes, args.date)
s_input_options = 's3a://{}/{}*_options.csv'.format(
    args.options, args.date)
s_output = 's3a://{}'.format(args.output)

options_table = process_options_data(spark, s_input_options, s_output)
options_table.printSchema()

(options_table.filter(options_table["optn_ua"] == 'B3SA3')
   .select('ticker', 'optn_ua', 'optn_expn_days', 'optn_expn_date', 'ua_mkt_bid', 'optn_mktbs_bid', 'optn_mktbs_ask')
   .show(5))


