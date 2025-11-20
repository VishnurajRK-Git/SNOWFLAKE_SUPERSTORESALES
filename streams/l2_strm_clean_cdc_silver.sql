/* This code corresponds to Capturing Changes in the table using stream. 

1. Stream to track changes in clean table - Silver stage (CDC - All Changes) 
*/

-- Create stream to capture data inserts into Raw table
CREATE OR REPLACE STREAM SUPERSTORE_SALES.L2_SILVER.L2_SS_SALES_CLEAN_CDC
ON TABLE SUPERSTORE_SALES.L2_SILVER.SS_SALES_CLEAN;