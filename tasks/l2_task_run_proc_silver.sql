/* This code corresponds to Task Scheduling 

1. Task-2 - Periodical Refresh of task - trigger Store procedure that transfer Data from Bronze to Silver 
2. Scheduled at Every day 5:15pm IST (UTC - 5:30)
3. Idealy this task is meant to execute after the task 1 but not explicitly defined

*/


-- ################## TASK-2 #########################
-- Task to load clean data into L2_Silver Stage table

CREATE OR REPLACE TASK SUPERSTORE_SALES.L1_BRONZE.L2_CLEAN_INSERT
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 45 11 */1 * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('SUPERSTORE_SALES.L1_BRONZE.L1_SS_Sales_RAW_CDC')
AS
CALL SUPERSTORE_SALES.L2_SILVER.L2_CLEAN_INS_UPD('SUPERSTORE_SALES.L1_BRONZE.L1_SS_Sales_RAW_CDC');