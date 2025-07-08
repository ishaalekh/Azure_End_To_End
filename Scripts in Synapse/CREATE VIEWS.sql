CREATE VIEW gold.Calendar
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Calendar/',
    FORMAT = 'PARQUET'
) AS query1;
CREATE VIEW gold.Customers
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Customers/',
    FORMAT = 'PARQUET'
) AS query2;
CREATE VIEW gold.Product_Categories
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Product_Categories/',
    FORMAT = 'PARQUET'
) AS query3;
CREATE VIEW gold.Product_Subcategories
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Product_Subcategories/',
    FORMAT = 'PARQUET'
) AS query4;

CREATE VIEW gold.Products
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Products/',
    FORMAT = 'PARQUET'
) AS query5;
CREATE VIEW gold.Returns
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Returns/',
    FORMAT = 'PARQUET'
) AS query6;
CREATE VIEW gold.Sales
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Sales/',
    FORMAT = 'PARQUET'
) AS query7;

CREATE VIEW gold.Territories
AS
SELECT 
* 
FROM 
OPENROWSET
(
    BULK 'https://portfolioprojectdata.blob.core.windows.net/silver/Territories/',
    FORMAT = 'PARQUET'
) AS query8;

