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

# # Athena querying

# For reference, some high-level instructions can be found in this [link](https://aws.amazon.com/blogs/machine-learning/run-sql-queries-from-your-sagemaker-notebooks-using-amazon-athena/).
#  Let's start by accessing the schema related to the quotes and options table.

# +
# %matplotlib inline
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyathena import connect

plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (10, 6)

# +
s3_bucket = "s3://data-from-etl/quotes.parquet/"

conn = connect(s3_staging_dir=s3_bucket,
               region_name='us-west-2')

# -

df = pd.read_sql("""
    SELECT *
      FROM processed.quotes
      LIMIT 8;
    """, conn)
df.info()

df = pd.read_sql("""
    SELECT *
      FROM processed.options
      LIMIT 8;
    """, conn)
df.info()

# Now, let's check the dates presented to query

df = pd.read_sql("""
    SELECT DISTINCT
        year, month, day
      FROM processed.quotes
      ORDER BY year, month, day ASC
      LIMIT 8;
    """, conn)
df

# Something to keep in mind is that Glue only recognizes partitions as strings if you are using the Glue
#  crawler to create your table from S3. So, if you want to perform some numerical filter using partitions
#  in your query, as below, you should edit the table schema in the AWS Glue console, or you have to create
#  and load the data using Boto3.

# +
df = pd.read_sql("""
    SELECT *
      FROM processed.quotes
      WHERE (
          ticker='PETR4'
          AND year=2020
          AND month=4
          AND day>=1
      )
      ORDER BY intdate ASC;
    """, conn)

df.tail()
# -

df.info()

ax = df[['bid', 'ask']].plot();

# The data stored has missing values and and has bid/ask prices collected
#  throughout each day. Let's plot the spread from two correlated assets.

# +
df = pd.read_sql("""
    SELECT intdate, ticker, bid, ask
      FROM processed.quotes
      WHERE (
          ticker='ITUB4'
          OR ticker='BBDC4'
      )
      ORDER BY intdate ASC;
    """, conn)

df = df.groupby(['intdate', 'ticker']).last().unstack().dropna()
df.index = pd.to_datetime(df.index, format='%Y%m%d%H%M')
df.tail()
# -

f_mult = 21.52/19.86
ax = ((df[('bid', 'ITUB4')] - df[('bid', 'BBDC4')]*f_mult)).plot(label='ask', legend=True)
((df[('ask', 'ITUB4')] - df[('ask', 'BBDC4')]*f_mult)).plot(label='bid', legend=True, ax=ax);

f_mult = 21.52/19.86
ax = ((df[('bid', 'ITUB4')] - df[('bid', 'BBDC4')]*f_mult)).reset_index(drop=True).plot(label='ask', legend=True)
((df[('ask', 'ITUB4')] - df[('ask', 'BBDC4')]*f_mult)).reset_index(drop=True).plot(label='bid', legend=True, ax=ax);

# Finally, let's check the options table

# +

df = pd.read_sql("""
    SELECT *
      FROM processed.options
      LIMIT 8;
    """, conn)
df
# -

df = pd.read_sql("""
    SELECT DISTINCT
        year, month, day
      FROM processed.options
      ORDER BY year, month, day ASC
      LIMIT 8;
    """, conn)
df


