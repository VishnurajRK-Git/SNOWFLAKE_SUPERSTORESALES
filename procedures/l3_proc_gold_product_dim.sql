/* This code corresponds to Data Transfer Silver to Gold - Product Dimension Table. 

1. Stored Procedure defined to insert/Merge data into Product Dim table

*/

-- Store Procedure to load into Product table
CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_PRODUCT_DIM(TMP_TBLE_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var product_dim = 'SUPERSTORE_SALES.L3_GOLD.SS_PRODUCT_DIM';
    var sql_load_Prd = `
        MERGE INTO ${product_dim} AS tgt
    USING (
      SELECT DISTINCT PRODUCT_ID, CATEGORY, SUB_CATEGORY, PRODUCT_NAME, METADATA$ISUPDATE
      FROM ${TMP_TBLE_NAME}
      WHERE PRODUCT_ID IS NOT NULL
        AND METADATA$ACTION IN ('INSERT')
    ) AS src
    ON tgt.PRODUCT_ID = src.PRODUCT_ID 
    WHEN MATCHED AND src.METADATA$ISUPDATE = 'TRUE' THEN
      UPDATE SET
        tgt.CATEGORY = src.CATEGORY,
        tgt.SUB_CATEGORY = src.SUB_CATEGORY,
        tgt.PRODUCT_NAME = src.PRODUCT_NAME
    WHEN NOT MATCHED THEN
      INSERT (PRODUCT_ID, CATEGORY, SUB_CATEGORY, PRODUCT_NAME)
      VALUES (src.PRODUCT_ID, src.CATEGORY, src.SUB_CATEGORY, src.PRODUCT_NAME)
  `;

    // execute the statement
    
    var stmt = snowflake.createStatement({sqlText: sql_load_Prd});
    var rs = stmt.execute();
    
    return `Product table updated.`;
    
} catch (err) { 
    return JSON.stringify({ status: 'ERROR', message: err.message });
}
$$;

--CALL L3_LOAD_PRODUCT_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP');

