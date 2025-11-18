/* This code corresponds to Data Transfer Silver to Gold - Date Dimension Table. 

1. Stored Procedure defined to insert/Merge data into Date Dim table

*/

-- Store Procedure to load values into date table
CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_DATE_DIM(TMP_TBLE_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var date_dim = 'SUPERSTORE_SALES.L3_GOLD.SS_DATE_DIM';
    var sql_load_date = `
    MERGE INTO ${date_dim} AS tgt
    USING (
      With f_dates AS (
      SELECT DISTINCT ORDER_DATE AS FULL_DATE FROM ${TMP_TBLE_NAME}
      WHERE ORDER_DATE IS NOT NULL AND METADATA$ACTION IN ('INSERT')
      UNION 
      SELECT DISTINCT SHIP_DATE AS FULL_DATE FROM ${TMP_TBLE_NAME}
      WHERE SHIP_DATE IS NOT NULL AND METADATA$ACTION IN ('INSERT')
      )
      
    SELECT DISTINCT FULL_DATE
    FROM f_dates
    ) 
    AS src   
    ON tgt.FULL_DATE = src.FULL_DATE
      
    WHEN NOT MATCHED THEN
      INSERT (FULL_DATE, YEAR, MONTH, MONTH_NAME, DAY, QUARTER, WEEKDAY, IS_WEEKEND)
      VALUES (src.FULL_DATE, YEAR(src.FULL_DATE), MONTH(src.FULL_DATE), MONTHNAME(src.FULL_DATE),
      DAY(src.FULL_DATE), QUARTER(src.FULL_DATE), DAYNAME(src.FULL_DATE), 
      IFF(DAYNAME(src.FULL_DATE) IN ('Sat', 'Sun'), 1, 0))
  `;
    // execute the statement
    
    var stmt = snowflake.createStatement({sqlText: sql_load_date});
    var rs = stmt.execute();
    
    return `Date table updated`;
  
} catch (err) { 
    return JSON.stringify({ status: 'ERROR', message: err.message });
}
$$;

-- CALL L3_LOAD_DATE_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP');