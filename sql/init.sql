\c kafka_db;
drop table if exists orders;
create table if not exists orders(
order_id VARCHAR PRIMARY KEY,
status VARCHAR,
category VARCHAR,
value NUMERIC,
city VARCHAR,
payment_method VARCHAR,
discount NUMERIC
);
