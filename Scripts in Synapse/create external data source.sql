CREATE EXTERNAL DATA SOURCE source_silver
WITH 
(
    LOCATION = 'https://portfolioprojectdata.blob.core.windows.net/silver',
    CREDENTIAL = credIsha
)
CREATE EXTERNAL DATA SOURCE source_gold
WITH 
(
    LOCATION = 'https://portfolioprojectdata.blob.core.windows.net/gold',
    CREDENTIAL = credIsha
)
CREATE EXTERNAL FILE FORMAT parquetFile
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
CREATE EXTERNAL TABLE gold.extSales
WITH(
    LOCATION = 'extSales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = parquetFile
) AS
SELECT * FROM gold.sales