 DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER;

DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS;


use warehouse compute_wh;
use database snowflake_learning_db;

create or replace table silver.customer_dim(
    customer_sk number autoincrement,
    customer_id number,
    state string,
    start_date date,
    end_date date,
    is_current string
);

--initial load

insert into silver.customer_dim(customer_id, state, start_date, end_date, is_current)
select
    c_customer_sk,
    ca.ca_state,
   current_date(),
   null,
    'Y'
    from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER c
    join SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS ca
    on c.c_current_addr_sk = ca.ca_address_sk;

    select count(*) from silver.customer_dim;

    select * from silver.customer_dim ;

    -- create a small stage 
    create or replace table silver.customer_stage as
    select customer_id, state from silver.customer_dim where is_current = 'Y';

    --mannual update one row
    update silver.customer_stage set state = 'TX' where customer_id = 7991230;

    --implement stage 2 using merge statement
    merge into silver.customer_dim target
    using silver.customer_stage source
    on target.customer_id= source.customer_id
    and target.is_current = 'Y'
    
    when matched and target.state <> source.state then
    update set target.end_date = current_date(), target.is_current = 'N'
    
    when not matched then
    insert (customer_id, state, start_date, end_date, is_current)
    values (source.customer_id, source.state, current_date(), null, 'Y');


    -- validate SCD behavior
    select * from silver.customer_dim where customer_id = 7991230 order by start_date