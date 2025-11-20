/* This code corresponds to Task Scheduling 

1. Task-1 - Periodical Refresh of data ingestion pipe
2. Scheduled at Every day 5:00pm IST (UTC - 5:30)

*/


-- Creating task to schedule periodic refresh in snow pipe
CREATE OR REPLACE TASK SUPERSTORE_SALES.L1_BRONZE.L1_Pipe_Refresh
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 30 11 */1 * * UTC'
AS
ALTER PIPE SUPERSTORE_SALES.L1_BRONZE.L1_LOAD refresh;
