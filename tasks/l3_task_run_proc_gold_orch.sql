/* This code corresponds to Task Scheduling 

1. Task-2 - Periodical Refresh of data ingestion pipe
2. Scheduled at Every day 5:15pm IST (UTC - 5:30)
3. Idealy this task is meant to execute after the task 1 but not explicitly defined

*/


-- Task to Transfer data from L2_Silver to L3_Gold (Executes Store Procedures)
-- Scheduled to run after L2_Silver table update
CREATE OR REPLACE TASK SUPERSTORE_SALES.L3_BRONZE.L3_LOAD_FACT_DIM_TABLE_TASK
WAREHOUSE = 'COMPUTE_WH'
AFTER SUPERSTORE_SALES.L1_BRONZE.L2_CLEAN_INSERT
WHEN SYSTEM$STREAM_HAS_DATA('SUPERSTORE_SALES.L2_SILVER.L2_SS_SALES_CLEAN_CDC')
AS
CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_DIM_FACT_L2_CDC('SUPERSTORE_SALES.L2_SILVER.L2_SS_SALES_CLEAN_CDC');
