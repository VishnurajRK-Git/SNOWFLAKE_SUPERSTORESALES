/* This code corresponds to Data Cleaning and Transfer. 

The following Code depicts the flow used for data transfer from Bronze to Silver
1. Insert/Merge function carries few data type corrections - example date format, float format
2. Stored Procedure defined to insert/Merge data into clean table

*/



-- Store Procedure to Insert or Update Silver Clean Table

CREATE OR REPLACE PROCEDURE SUPERSTORE_SALES.L2_SILVER.L2_CLEAN_INS_UPD(STREAM_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    var clean_table = 'SUPERSTORE_SALES.L2_SILVER.SS_SALES_CLEAN'
    var sql_procedure = `
    MERGE INTO ${clean_table} Trgt
    USING (
      SELECT DISTINCT
        ORDER_ID, CUSTOMER_ID, PRODUCT_ID, ORDER_DATE, SHIP_DATE, SHIP_MODE,
        CUSTOMER_NAME, SEGMENT, COUNTRY, CITY, STATE, POSTAL_CODE, REGION_AREA,
        CATEGORY, SUB_CATEGORY, PRODUCT_NAME, SALES, QUANTITY, DISCOUNT, PROFIT
      FROM ${STREAM_NAME}
    ) Srce
    ON Trgt.ORDER_ID = Srce.ORDER_ID AND
    Trgt.CUSTOMER_ID = Srce.CUSTOMER_ID AND
    Trgt.PRODUCT_ID = Srce.PRODUCT_ID
    WHEN MATCHED
        THEN UPDATE SET 
                Trgt.ORDER_DATE = COALESCE(
                            TRY_TO_DATE(Srce.ORDER_DATE, 'MM-DD-YYYY'),
                            TRY_TO_DATE(Srce.ORDER_DATE, 'MM/DD/YYYY'),
                            TRY_TO_DATE(Srce.ORDER_DATE, 'M/D/YYYY'),
                            TRY_TO_DATE(Srce.ORDER_DATE, 'YYYY-MM-DD')
                          ),
                Trgt.SHIP_DATE = COALESCE(
                            TRY_TO_DATE(Srce.SHIP_DATE, 'MM-DD-YYYY'),
                            TRY_TO_DATE(Srce.SHIP_DATE, 'MM/DD/YYYY'),
                            TRY_TO_DATE(Srce.SHIP_DATE, 'M/D/YYYY'),
                            TRY_TO_DATE(Srce.SHIP_DATE, 'YYYY-MM-DD')
                          ),
                Trgt.Customer_Name = Srce.Customer_Name,
                Trgt.Segment = Srce.Segment,
                Trgt.Country = Srce.Country,
                Trgt.City = Srce.City,
                Trgt.State = Srce.State,
                Trgt.Postal_Code = Srce.Postal_Code,
                Trgt.Region_Area = Srce.Region_Area,
                Trgt.Category = Srce.Category,
                Trgt.Sub_Category = Srce.Sub_Category,
                Trgt.Product_Name = Srce.Product_Name,
                Trgt.Sales = ROUND(TRY_TO_DOUBLE(Srce.Sales), 2),
                Trgt.Quantity = Srce.Quantity,
                Trgt.Discount = ROUND(TRY_TO_DOUBLE(Srce.Discount), 2),
                Trgt.Profit = ROUND(TRY_TO_DOUBLE(Srce.Profit), 2)    
    WHEN NOT MATCHED
        THEN INSERT (
                Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment,
                Country, City, State, Postal_Code, Region_Area, Product_ID,
                Category, Sub_Category, Product_Name, Sales, Quantity,
                Discount, Profit)
                VALUES 
                (Srce.Order_ID, 
                -- Using Coalesce and Try to date converting multiple date format to single date format
                COALESCE(
                    TRY_TO_DATE(Srce.Order_Date, 'MM-DD-YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'MM/DD/YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'M/D/YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'YYYY-MM-DD')
                  ), 
                COALESCE(
                    TRY_TO_DATE(Srce.Order_Date, 'MM-DD-YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'MM/DD/YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'M/D/YYYY'),
                    TRY_TO_DATE(Srce.Order_Date, 'YYYY-MM-DD')
                  ), 
                Srce.Ship_Mode, Srce.Customer_ID, Srce.Customer_Name, Srce.Segment,
                Srce.Country, Srce.City, Srce.State, Srce.Postal_Code, Srce.Region_Area, Srce.Product_ID,
                Srce.Category, Srce.Sub_Category, Srce.Product_Name, 
                ROUND(TRY_TO_DOUBLE(Srce.Sales), 2), 
                Srce.Quantity, Srce.Discount, 
                ROUND(TRY_TO_DOUBLE(Srce.Profit), 2)
                )
    `;

    // Execute the statement
    var sql_stmt = snowflake.createStatement({sqlText: sql_procedure});
    sql_stmt.execute();

    return 'Silver Table updated';
}
catch (err) {
    return JSON.stringify({STATUS: 'ERROR', message: err.message});
}
$$;

-- CALL SUPERSTORE_SALES.L2_SILVER.L2_CLEAN_INS_UPD('SUPERSTORE_SALES.L1_BRONZE.L1_SS_Sales_RAW_CDC');
