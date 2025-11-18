/* This code corresponds to Data Transfer Silver to Gold - Orchestration
stored procedure

1. Stored Procedure defined to call all L3 store procedure
  -- Transient table created to hold the data from L2 stream
  -- Call Product Dim table procedure
  -- Call Customer Dim table procedure
  -- Call Shipment Dim table procedure
  -- Call Date Dim table procedure
  -- Call Order fact table procedure

*/

-- Main Store Procedure to Call All Other Store Procedure

CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_DIM_FACT_L2_CDC(STREAM_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    if (!STREAM_NAME) {
    return JSON.stringify({ status: 'ERROR', message: err.message });
  }
    // Consume stream data into temp table
    var sql_str_temp = `
        CREATE OR REPLACE TRANSIENT TABLE SUPERSTORE_SALES.L3_GOLD.STR_TEMP AS
        SELECT * FROM ${STREAM_NAME}
        WHERE METADATA$ACTION != 'DELETE'
    `;
    
    var stmt1 = snowflake.createStatement({sqlText: sql_str_temp});
    stmt1.execute();

    var st_count = snowflake.createStatement({ sqlText: 'SELECT COUNT(*) AS CNT FROM STR_TEMP' });
    st_count.execute();
    var rs = st_count.execute();
    rs.next();
    var cnt = rs.getColumnValue(1);

    if (cnt > 1) {
    // Execute Product Table Stored Procedure
    var sql_call = `CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_PRODUCT_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP')`;
    var stmt = snowflake.createStatement({sqlText: sql_call});
    stmt.execute();

    // Execute Customer Table Stored Procedure
    var sql_call = `CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_CUSTOMER_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP')`;
    var stmt = snowflake.createStatement({sqlText: sql_call});
    stmt.execute();

    // Execute Shipment Table Stored Procedure
    var sql_call = `CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_SHIPMENT_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP')`;
    var stmt = snowflake.createStatement({sqlText: sql_call});
    stmt.execute();

    // Execute Date Table Stored Procedure
    var sql_call = `CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_DATE_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP')`;
    var stmt = snowflake.createStatement({sqlText: sql_call});
    stmt.execute();

    // Execute Order Fact Table Stored Procedure
    var sql_call = `CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_FACT('SUPERSTORE_SALES.L3_GOLD.STR_TEMP')`;
    var stmt = snowflake.createStatement({sqlText: sql_call});
    stmt.execute();

    }

    return 'Dimension and Fact Tables Updated';
}
catch (err) { 
    return JSON.stringify({ status: 'ERROR', message: err.message });
}
$$;

-- CALL SUPERSTORE_SALES.L3_GOLD.L3_LOAD_DIM_FACT_L2_CDC('SUPERSTORE_SALES.L2_SILVER.L2_SS_SALES_CLEAN_CDC');