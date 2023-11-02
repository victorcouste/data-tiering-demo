----- Load  Into PostgreSQL
create schema postgresql.victorc_demo;

CREATE TABLE postgresql.victorc_demo.credit_card_payment(
   cc_number varchar,
   payment_date varchar,
   payment_amount double,
   payment_due_date varchar,
   delinquent_payment varchar(1),
   balance double,
   payment_year integer
);

insert into postgresql.victorc_demo.credit_card_payment
select 
   cc_number,
   payment_date,
   payment_amount,
   payment_due_date,
   delinquent_payment,
   balance,
   CAST (SUBSTR(payment_date, 1, 4) AS INTEGER) as payment_year
from "postgresql"."burst_bank_large"."credit_card_payment";

select count (*) from postgresql.victorc_demo.credit_card_payment;

-- Create cold data table in Data Lake

CREATE TABLE hive.victorc_demo.credit_card_payment_cold
(
    cc_number varchar,
    payment_date varchar,
    payment_amount double,
    payment_due_date varchar,
    delinquent_payment varchar(1),
    balance double,
    payment_year integer
)
WITH (
    external_location = 's3://victorc-data/datalake/cold/credit_card_payment/',
    format = 'PARQUET',
    partitioned_by = ARRAY['payment_year']
);

--- Warm to cold - move data from Postgres to cold data lake storage

insert into hive.victorc_demo.credit_card_payment_cold
select *
from postgresql.victorc_demo.credit_card_payment
 where payment_year < 2021;

-- Delete colded data from postgresql

delete from postgresql.victorc_demo.credit_card_payment
 where payment_year < 2021;
 
-- Check warm and cold data ----

SELECT count(*) from postgresql.burst_bank_large.credit_card_payment; 
select ( (select count(*) from postgresql.victorc_demo.credit_card_payment) +
 (select count(*) from hive.victorc_demo.credit_card_payment_cold));

select payment_year, count(*) from postgresql.victorc_demo.credit_card_payment group by payment_year;

-- View to query all data, warm and cold ----

create or replace view hive.victorc_demo.view_credit_card_payment
as 
select * from postgresql.victorc_demo.credit_card_payment as warm_payments  where payment_year >YEAR(current_date)-3 
union all 
select * from hive.victorc_demo.credit_card_payment_cold as cold_payments where payment_year <=YEAR(current_date)-3;

--------- DEMONSTRATION ----------------

select payment_year, count(*) as nb_payments from postgresql.victorc_demo.credit_card_payment group by payment_year order by payment_year;
select payment_year, count(*) as nb_payments from hive.victorc_demo.credit_card_payment_cold group by payment_year order by payment_year;

select *
from hive.victorc_demo.view_credit_card_payment
where payment_year = 2020;

select payment_year, count(*) as nb_payments
from hive.victorc_demo.view_credit_card_payment
where payment_year between 2017 and 2020
group by payment_year
order by payment_year;
