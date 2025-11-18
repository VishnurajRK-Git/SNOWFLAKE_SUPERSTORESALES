# SNOWFLAKE_SUPERSTORESALES

Here We use SuperStore Sales data From Kaggle.

Repo layout:
- db_setup/       -- DDL and schema creation
 
    The following Code depicts the building database setup
    DB
    --- L1_Bronze
        --- RAW TABLE
    --- L2_Silver
        --- CLEAN TABLE
    --- L3_GOLD (Schema with Dimension and facts table)
        --- CUSTOMER DIM TABLE
        --- PRODUCT DIM TABLE
        --- SHIPMENT DIM TABLE
        --- DATE DIM TABLE
        --- ORDER FACT TABLE

- procedures/     -- stored procedures

    The following Code depicts the store procedure definitions for data transfer
    1. L1_Bronze --> L2_Silver
    2. L2_Silver --> L3_Gold

- streams/        -- stream and pipe definitions

    The following Code depicts the snowpipe definition and stream definition to capture table changes
    1. snowpipe - data ingestion
    2. l1_stream - CDC of Bronze table
    3. l2_stream - CDC of Silver table

- tasks/          -- tasks definitions
    The following Code depicts the task definition to call stored procedure
    1. Task1 - pipe refresh
    2. Task2 - Data transfer Bronze to Silver
    3. Task3 - Data transfer Silveer to Gold

- pipelines/      -- orchestration & CI scripts