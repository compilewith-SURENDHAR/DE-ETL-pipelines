-- CREATE VEIW CALENDER

CREATE VIEW gold.calender
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Calendar/',
        FORMAT = 'PARQUET'
) AS QUER1


-- CREATE VEIW CUSTOMERS

CREATE VIEW gold.customers
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
) AS QUER2



-- CREATE VEIW PRODUCTS

CREATE VIEW gold.products
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
) AS QUER3


-- CREATE VEIW RETURNS

CREATE VIEW gold.returns
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Returns/',
        FORMAT = 'PARQUET'
) AS QUER4


-- CREATE VEIW SALES

CREATE VIEW gold.sales
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Sales/',
        FORMAT = 'PARQUET'
) AS QUER5


-- CREATE VEIW SUBCATEGORIES

CREATE VIEW gold.subcategories
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_SubCategories/',
        FORMAT = 'PARQUET'
) AS QUER6


-- CREATE VEIW TERRITORIES

CREATE VIEW gold.territiores
AS 
SELECT *
FROM 
OPENROWSET(BULK 'https://awsstoragedatalake2004.blob.core.windows.net/silver/AdventureWorks_Territories/',
        FORMAT = 'PARQUET'
) AS QUER7