USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

--adding transaction date to silver fact
DROP TABLE IF EXISTS SILVER.CUSTOMER_DIM;

CREATE OR REPLACE TABLE SILVER.CUSTOMER_DIM (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,
    state STRING,
    start_date DATE,
    end_date DATE,
    is_current STRING
);

INSERT INTO SILVER.CUSTOMER_DIM
(customer_id, state, start_date, end_date, is_current)
SELECT
    c.c_customer_sk,
    ca.ca_state,
    '1900-01-01',
    NULL,
    'Y'
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER c
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS ca
    ON c.c_current_addr_sk = ca.ca_address_sk;

   CREATE OR REPLACE TABLE SILVER.FACT_SALES AS
SELECT
    d.customer_sk,
    f.ss_store_sk AS store_id,
    f.ss_sales_price AS sales_price,
    f.transaction_date
FROM SILVER.STORE_SALES_ENRICHED f
JOIN SILVER.CUSTOMER_DIM d
    ON f.ss_customer_sk = d.customer_id
    AND f.transaction_date >= d.start_date
    AND (f.transaction_date < d.end_date OR d.end_date IS NULL);

    SELECT * FROM SILVER.FACT_SALES;