/* This code corresponds to Data Transfer Silver to Gold - Shipment Dimension Table. 

1. Stored Procedure defined to insert/Merge data into Shipment Dim table

*/

-- Store Procedure to load values into Shipment table
CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_SHIPMENT_DIM(TMP_TBLE_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var ship_dim = 'SUPERSTORE_SALES.L3_GOLD.SS_SHIPMENT_DIM';
    var sql_load_shp = `
        MERGE INTO ${ship_dim} AS tgt
    USING (
      SELECT DISTINCT SHIP_MODE
      FROM ${TMP_TBLE_NAME}
      WHERE ORDER_ID IS NOT NULL
        AND METADATA$ACTION IN ('INSERT')
    ) AS src
    ON UPPER(tgt.SHIP_MODE) = UPPER(src.SHIP_MODE)
      
    WHEN NOT MATCHED THEN
      INSERT (SHIP_MODE)
      VALUES (src.SHIP_MODE)
  `;

    // execute the statement
    
    var stmt = snowflake.createStatement({sqlText: sql_load_shp});
    var rs = stmt.execute();
    
    return `Shipment table updated.`;

} catch (err) { 
    return JSON.stringify({ status: 'ERROR', message: err.message });
}
$$;

--CALL L3_LOAD_SHIPMENT_DIM('SUPERSTORE_SALES.L3_GOLD.STR_TEMP');