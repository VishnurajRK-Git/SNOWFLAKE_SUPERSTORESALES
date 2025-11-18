/* This code corresponds to Data Ingestion using snowpipe. 
Here We use SuperStore Sales data From Kaggle. 

The following Code depicts the flow used for data ingestion
1. Defined File Format is created 
2. Internal Stage used to Load data in snow flake
3. Snow pipe for continuous loading into SS_Sales_Raw table uses copy into function
*/

-- Creating file format
CREATE or REPLACE FILE FORMAT SUPERSTORE_SALES.L1_BRONZE.FL_CSV
TYPE = 'csv'
SKIP_HEADER = 1
TRIM_SPACE = TRUE
SKIP_BLANK_LINES = TRUE
DATE_FORMAT = AUTO
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('', 'NULL')
EMPTY_FIELD_AS_NULL = TRUE;

-- Creating Internal Stage.
CREATE or REPLACE STAGE SUPERSTORE_SALES.L1_BRONZE.L1_STG
FILE_FORMAT = 'SUPERSTORE_SALES.L1_BRONZE.FL_CSV';

/* 
Lets Load Data into stage using the PUT command
PUT file://xxxx @L1_STG AUTO_COMPRESS=FALSE
Use snowsql to execute PUT and GET command 
*/

-- Auto Ingestion Pipeline using snow pipe
CREATE OR REPLACE PIPE SUPERSTORE_SALES.L1_BRONZE.L1_LOAD
AS
COPY INTO SUPERSTORE_SALES.L1_BRONZE.SS_SALES_RAW FROM @SUPERSTORE_SALES.L1_BRONZE.L1_STG/Input_csv/ Pattern='.*\.csv' 
ON_ERROR = 'CONTINUE';