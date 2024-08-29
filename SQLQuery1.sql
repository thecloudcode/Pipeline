-- create database futurense;
-- use futurense;

-- create table campaign_performance(
-- 	dates date,
-- 	campaign_name nvarchar(100),
-- 	campaign_start_date date,
-- 	creative_name nvarchar(100),
-- 	total_spent float,
-- 	impressions int,
-- 	clicks int, 
-- 	click_through_rate float, 
-- 	leads float,
-- 	campaign_platform nvarchar(100),
-- 	adset_name nvarchar(100)	
-- );
use futurense;

-- alter table campaign_performance alter column campaign_name text;
-- alter table campaign_performance alter column adset_name text;

select * from campaign_performance_live_api;



-- delete campaign_performance;