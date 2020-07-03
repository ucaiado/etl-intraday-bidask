class SqlQueries:
    quotes_create_database = "CREATE DATABASE IF NOT EXISTS processed;"

    quotes_drop_table = "DROP TABLE IF EXISTS processed.quotes"

    quotes_create_table = ("""
        CREATE EXTERNAL TABLE IF NOT EXISTS processed.quotes (
            `intdate` bigint,
            `ticker` string,
            `last_close` float,
            `open` float,
            `max` float,
            `min` float,
            `last` float,
            `qty_bid` int,
            `bid` float,
            `ask` float,
            `qty_ask` int,
            `num_trades` int,
            `qty_total` int,
            `total_volume` float
        )
        PARTITIONED BY (
            `year` int,
            `month` int,
            `day` int)
        ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
          'serialization.format' = '1'
        ) LOCATION '{}'
        TBLPROPERTIES (
            'classification'='parquet',
            'has_encrypted_data'='false',
            'typeOfData'='file'
        );
    """)

    quotes_rapair = "MSCK REPAIR TABLE processed.quotes;"

    quotes_load = ("""
            ALTER TABLE processed.quotes ADD IF NOT EXISTS
            PARTITION (
                year={},
                month={},
                day={}
            );
        """)

    options_create_database = "CREATE DATABASE IF NOT EXISTS processed;"

    options_drop_table = "DROP TABLE IF EXISTS processed.options"

    options_create_table = ("""
        CREATE EXTERNAL TABLE IF NOT EXISTS processed.options (
            `intdate` bigint,
            `ticker` string,
            `optn_ua` string,
            `optn_style` string,
            `optn_tp` string,
            `optn_expn_days` int,
            `optn_expn_date` string,
            `optn_strike` float,
            `optn_bs_delta` float,
            `optn_mkt_bid` float,
            `optn_bs_bid` float,
            `optn_mktbs_bid` float,
            `ua_mkt_bid` float,
            `optn_mkt_ask` float,
            `optn_bs_ask` float,
            `optn_mktbs_ask` float,
            `ua_mkt_ask` float,
            `bid_volimp` float,
            `bid_volcone` float,
            `ask_volimp` float,
            `ask_volcone` float
        )
        PARTITIONED BY (
            `year` int,
            `month` int,
            `day` int)
        ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
          'serialization.format' = '1'
        ) LOCATION '{}'
        TBLPROPERTIES (
            'classification'='parquet',
            'has_encrypted_data'='false',
            'typeOfData'='file'
        );
    """)

    options_rapair = "MSCK REPAIR TABLE processed.options;"

    options_load = ("""
            ALTER TABLE processed.options ADD IF NOT EXISTS
            PARTITION (
                year={},
                month={},
                day={}
            );
        """)
