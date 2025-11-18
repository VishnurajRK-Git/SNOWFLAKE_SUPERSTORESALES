/* This code corresponds to Data Transfer Silver to Gold - Order Fact Table. 

1. Stored Procedure defined to insert/Merge data into Orders Fact table

*/

-- Store Procedure to load Fact table data
CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L3_GOLD.L3_LOAD_FACT(TMP_TBLE_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var fact_table = 'SUPERSTORE_SALES.L3_GOLD.SS_ORDER_FACT';
    var sql_load_fact = `
    MERGE INTO ${fact_table} AS tgt
    USING ( SELECT DISTINCT
    tmp.ORDER_ID, tmp.ORDER_DATE, tmp.SHIP_DATE,
    shp_dim.SHIP_MODE_ID, tmp.CUSTOMER_ID, tmp.PRODUCT_ID, tmp.SALES,
    tmp.QUANTITY, tmp.DISCOUNT, tmp.PROFIT, tmp.METADATA$ISUPDATE
    FROM ${TMP_TBLE_NAME} tmp
    LEFT JOIN SS_SHIPMENT_DIM shp_dim
    ON shp_dim.SHIP_MODE = tmp.SHIP_MODE
    WHERE tmp.METADATA$ACTION = 'INSERT'
    ) AS src
    ON src.ORDER_ID = tgt.ORDER_ID AND
    src.CUSTOMER_ID = tgt.CUSTOMER_ID AND
    src.PRODUCT_ID = tgt.PRODUCT_ID
    
    WHEN MATCHED AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
    tgt.ORDER_DATE = src.ORDER_DATE,
    tgt.SHIP_DATE = src.SHIP_DATE,
    tgt.SHIP_MODE_ID = src.SHIP_MODE_ID,
    tgt.SALES = src.SALES,
    tgt.QUANTITY = src.QUANTITY,
    tgt.DISCOUNT = src.DISCOUNT,
    tgt.PROFIT = src.PROFIT,
    tgt.REVENUE_BEFORE_DISCOUNT = ROUND(src.SALES/NULLIF(1-src.DISCOUNT,0),2),
    tgt.PROFIT_MARGIN = ROUND(src.PROFIT/(src.SALES/NULLIF(1-src.DISCOUNT,0)),3),
    tgt.SHIP_DURATION = DATEDIFF(DAY,src.ORDER_DATE, src.SHIP_DATE),
    tgt.HIGH_DISCOUNT_FLAG = IFF (src.DISCOUNT > 0.20,TRUE,FALSE)

    WHEN NOT MATCHED THEN
    INSERT (
    ORDER_ID, ORDER_DATE, SHIP_DATE, SHIP_MODE_ID, CUSTOMER_ID,
    PRODUCT_ID, SALES, QUANTITY, DISCOUNT, PROFIT, REVENUE_BEFORE_DISCOUNT,
    PROFIT_MARGIN, SHIP_DURATION, HIGH_DISCOUNT_FLAG) 
    VALUES (
    src.ORDER_ID, src.ORDER_DATE, src.SHIP_DATE,
    src.SHIP_MODE_ID,src.CUSTOMER_ID, src.PRODUCT_ID, src.SALES,
    src.QUANTITY, src.DISCOUNT, src.PROFIT,
    ROUND(src.SALES/NULLIF(1-src.DISCOUNT,0),2),
    ROUND(src.PROFIT/(src.SALES/NULLIF(1-src.DISCOUNT,0)),3),
    DATEDIFF(DAY,src.ORDER_DATE, src.SHIP_DATE),
    IFF (src.DISCOUNT > 0.20,TRUE,FALSE)   
    )
    `;

    // execute the statement
    var stmt = snowflake.createStatement({sqlText: sql_load_fact});
    var rs = stmt.execute();

    return `Fact table updated.`;
} catch (err) {
    return JSON.stringify({status: 'Error', message: err.message});
}
$$;

-- CALL L3_LOAD_FACT('SUPERSTORE_SALES.L3_GOLD.STR_TEMP');