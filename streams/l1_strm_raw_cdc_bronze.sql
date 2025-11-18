/* This code corresponds to Capturing Changes in the table using stream. 

1. Stream to track changes in raw table - Bronze stage (CDC - Append only) 
*/

-- Create stream to capture data inserts into Raw table
CREATE OR REPLACE STREAM SUPERSTORE_SALES.L1_BRONZE.L1_SS_Sales_RAW_CDC
ON TABLE SUPERSTORE_SALES.L1_BRONZE.SS_SALES_RAW
APPEND_ONLY = TRUE;
