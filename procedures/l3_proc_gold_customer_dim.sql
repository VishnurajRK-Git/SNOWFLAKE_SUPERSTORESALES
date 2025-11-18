/* This code corresponds to Data Transfer Silver to Gold - Customer Dimension Table. 

1. Stored Procedure defined to insert/Merge data into Customer Dim table

*/



-- Store Procedure to load into Customer table
CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_CUSTOMER_DIM(TMP_TBLE_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var customer_dim = 'SUPERSTORE_SALES.L3_GOLD.SS_CUSTOMER_DIM';
    var sql_load_Cust = `
        MERGE INTO ${customer_dim} AS tgt
    USING (
      SELECT DISTINCT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT, COUNTRY, CITY, STATE, POSTAL_CODE, REGION_AREA, METADATA$ISUPDATE
      FROM ${TMP_TBLE_NAME}
      WHERE CUSTOMER_ID IS NOT NULL
        AND METADATA$ACTION IN ('INSERT')
    ) AS src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
    WHEN MATCHED AND src.METADATA$ISUPDATE = TRUE THEN
      UPDATE SET
        tgt.CUSTOMER_NAME = src.CUSTOMER_NAME,
        tgt.SEGMENT = src.SEGMENT,
        tgt.COUNTRY = src.COUNTRY,
        tgt.CITY = src.CITY,
        tgt.STATE = src.STATE,
        tgt.POSTAL_CODE = src.POSTAL_CODE,
        tgt.REGION_AREA = src.REGION_AREA
    WHEN NOT MATCHED THEN
      INSERT (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT, COUNTRY, CITY, STATE, POSTAL_CODE, REGION_AREA)
      VALUES (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.SEGMENT, src.COUNTRY, src.CITY, src.STATE, src.POSTAL_CODE, src.REGION_AREA)
  `;

    // execute the statement
    
    var stmt = snowflake.createStatement({sqlText: sql_load_Cust});
    var rs = stmt.execute();
    
    return `Customer table updated.`;
    
} catch (err) { 
    return JSON.stringify({ status: 'ERROR', message: err.message });
}
$$;

--CALL L3_LOAD_CUSTOMER_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP');