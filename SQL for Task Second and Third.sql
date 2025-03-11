
---find tables created by Python
select schemaname, tablename from pg_tables where tablename in ('receipts', 'items', 'users', 'brands');
select * from receipts limit 10;
select * from items limit 10;
select * from users limit 10;
select * from brands limit 10;


------------- THIRD: EVALUATE DATA QUALITY ISSUES IN THE DATA PROVIDED -------------
-- Note: Although 'Evaluate Data Quality' is listed as the third task, it is necessary to study the data and conduct a data quality evaluation before developing the New Structured Relational Data Model
--       (the first task) and answering predetermined questions from business stakeholders (the second task). Therefore, I have prioritized this task and moved it up in the workflow.

-- Individual Table Data Quality Evaluation
-- This section evaluates the quality of data within each individual table, identifying issues such as duplicates, missing values, and inconsistencies.

-----receipts
--------------------***************************************************************-------------------------------------
select * from receipts limit 10;
select count(1), count(distinct receipt_id) from receipts;
--1119,1119---this indicates that receipt_id is unique in the table

select create_date::date, count(1), count(distinct receipt_id) from receipts group by 1 order by 1;
---this is to check how the receipt create dates are distributed
--------------------***************************************************************-------------------------------------


-----users
--------------------***************************************************************-------------------------------------
select * from users limit 10;
select count(1), count(distinct user_id) from users;
--495,212---this indicates that user_id is NOT unique in the table

---find duplicates
select user_id, count(1) from users group by 1 having count(1) > 1;
-- 5ff5d15aeb7c7d12096d91a2
-- 5ff4ce33c3d63511e2a484b6
-- 600f008f4329897eac237bd8

select * from users where user_id = '5ff5d15aeb7c7d12096d91a2';

---finding: a lot of duplicates in user table
select count(1), count(distinct user_id)
from (
select distinct user_id, state, created_date, last_login, role, active, sign_up_source
from users
);
--212,212---this indicates that all duplicated user_ids are purely duplicated records (identical across all columns)

---dedup the users table
select distinct user_id, state, created_date, last_login, role, active, sign_up_source
into users_dedup
from users;

select count(1), count(distinct user_id) from users_dedup;
--212,212---indicates that Users table is deduplicated successfully
--------------------***************************************************************-------------------------------------


-----items
--------------------***************************************************************-------------------------------------
select * from items limit 100;
select count(1), count(distinct receipt_id), count(distinct receipt_id||partner_item_id) from items;
--6941,679,6941---this indicates that receipt_id and partner_item_id combination is the unique key
--------------------***************************************************************-------------------------------------


-----brands
--------------------***************************************************************-------------------------------------
select * from brands limit 10;
select count(1), count(distinct brand_id), count(distinct barcode) from brands;
--1167,1167,1160---this indicates that brand_id is unique in the table and barcode is NOT unique in the table

---find duplicates
select barcode, count(1) from brands group by 1 having count(1) > 1;
-- 511111605058
-- 511111204923
-- 511111704140
select count(1) cnt_barcodes_assigned_to_multi_brand_id from (select barcode, count(1) from brands group by 1 having count(1) > 1);
--7

select * from brands where barcode in (select barcode from brands group by 1 having count(1) > 1) order by barcode;
-- Upon reviewing the details, we discovered that 7 barcodes were mistakenly assigned to more than one brand.

---From the data, we are unable to determine which brand is the correct one to assign to the barcodes. Since the brand_id cannot be found in any other tables, we have to rely on the barcode for joining.
-- Before consulting with the subject matter expert, we should exclude these seven barcodes from our table to ensure the data remains clean.
select *
into brands_cleaned
from brands
where barcode not in (select barcode from brands group by 1 having count(1) > 1);

select count(1), count(distinct brand_id), count(distinct barcode) from brands_cleaned;
--1153,1153,1153---This indicates that the brand table has been cleaned, and the barcode is now ready to be used for joining.
--------------------***************************************************************-------------------------------------


-----This is how to join the tables. These joins are also documented in the ERD.
select *
from receipts a
left join users_dedup b on a.user_id = b.user_id
left join items c on a.receipt_id = c.receipt_id
left join brands_cleaned d on c.barcode = d.barcode
where a.receipt_id = '5ff473b10a7214ada10005c4'
limit 100;




-- Table Joins Data Quality Evaluation
-- This section evaluates the quality of data when joining tables, focusing on linkage completeness, missing relationships, and potential mismatches between tables.

-----receipts vs items
--------------------***************************************************************-------------------------------------
---receipts left join items
select count(1) total_rows_in_receipts,
       count(case when a.receipt_id is not null then 1 end) total_non_null_receipt_ids_in_receipts,
       count(distinct a.receipt_id) distinct_receipt_ids_in_receipts,
       count(case when b.receipt_id is not null then 1 end) total_non_null_receipt_ids_in_items,
       count(distinct b.receipt_id) distinct_receipt_ids_in_items
from receipts a
left join items b on a.receipt_id = b.receipt_id;
--7381,7381,1119,6941,679

---items left join receipts
select count(1) total_rows_in_items,
       count(case when a.receipt_id is not null then 1 end) total_non_null_receipt_ids_in_items,
       count(distinct a.receipt_id) distinct_receipt_ids_in_items,
       count(case when b.receipt_id is not null then 1 end) total_non_null_receipt_ids_in_receipts,
       count(distinct b.receipt_id) distinct_receipt_ids_in_items
from items a
left join receipts b on a.receipt_id = b.receipt_id;
--6941,6941,679,6941,679
-- Takeaway:
-- Among 1119 receipts, only 679 have corresponding item records in items.
-- All 679 receipts from items exist in receipts.
-- All records in both tables have receipt_id.
--------------------***************************************************************-------------------------------------

---receipts vs users
--------------------***************************************************************-------------------------------------
---receipts left join users_dedup
select count(1) total_rows_in_receipts,
       count(case when a.user_id is not null then 1 end) total_non_null_user_ids_in_receipts,
       count(distinct a.user_id) distinct_user_ids_in_receipts,
       count(case when b.user_id is not null then 1 end) total_non_null_user_ids_in_users_dedup,
       count(distinct b.user_id) distinct_user_ids_in_users_dedup
from receipts a
left join users_dedup b on a.user_id = b.user_id;
--1119,1119,258,971,141

---users_dedup left join receipts
select count(1) total_rows_in_users_dedup,
       count(case when a.user_id is not null then 1 end) total_non_null_user_ids_in_users_dedup,
       count(distinct a.user_id) distinct_user_ids_in_users_dedup,
       count(case when b.user_id is not null then 1 end) total_non_null_user_ids_in_receipts,
       count(distinct b.user_id) distinct_user_ids_in_receipts
from users_dedup a
left join receipts b on a.user_id = b.user_id;
--1042,1042,212,971,141
-- Takeaway:
-- Among 258 users in receipts, only 141 have records in users_dedup.
-- 71 (212 - 141) users in users_dedup have no receipts.
-- All records in both tables have user_id.
--------------------***************************************************************-------------------------------------

---items vs brands
--------------------***************************************************************-------------------------------------
---items left join brands_cleaned
select count(1) total_rows_in_items,
       count(case when a.barcode is not null then 1 end) total_non_null_barcodes_in_items,
       count(distinct a.barcode) distinct_barcodes_in_items,
       count(case when b.barcode is not null then 1 end) total_non_null_barcodes_in_brands,
       count(distinct b.barcode) distinct_barcodes_in_brands
from items a
left join brands_cleaned b on a.barcode = b.barcode;
--6941,3090,568,75,15

---brands_cleaned left join items
select count(1) total_rows_in_brands,
       count(case when a.barcode is not null then 1 end) total_non_null_barcodes_in_brands,
       count(distinct a.barcode) distinct_barcodes_in_brands,
       count(case when b.barcode is not null then 1 end) total_non_null_barcodes_in_items,
       count(distinct b.barcode) distinct_barcodes_in_items
from brands_cleaned a
left join items b on a.barcode = b.barcode;
--1213,1213,1153,75,15
-- Takeaway:
-- Among 568 barcodes in items, only 15 have records in brands.
-- 1138 (1153 - 15) barcodes in brands have no items.
-- Only 3090 of 6941 items records have barcodes.
-- All brands records have barcodes.
--------------------***************************************************************-------------------------------------

----MAIN DATA QUALITY ISSUES:
/*--------------------***************************************************************-------------------------------------
Duplicate Records in users Table
Issue: The users table contains 283 (495 - 212) duplicate rows, inflating the dataset. These are exact duplicates (same user_id, state, etc.), suggesting data ingestion or ETL errors.
Impact: Overstated user counts in analyses unless deduplicated (e.g., in joins with receipts).
Resolution: I’ve addressed this by creating users_dedup, which should replace users in downstream use.

Duplicate Barcodes in brands Table
Issue: 7 barcodes are assigned to multiple brands (14 rows total), indicating data inconsistency or misassignment. Without external validation, the correct brand per barcode is unclear.
Impact: Joining items to brands on barcode could lead to ambiguous or incorrect matches (multiple brands per item).
Resolution: I’ve created brands_cleaned to exclude these, a pragmatic interim fix until SME input clarifies true mappings.
Ideally, we should populate the brand_id in other tables, such as items, so that it can be used to join with the brands table more effectively.

Incomplete Linkage Between Tables
Receipts vs. Items:
Issue: 440 (1119 - 679) receipts lack item details, suggesting missing data in items or incomplete receipt scanning.
Impact: Analyses requiring item-level data (e.g., spend by product) are limited to 60.7% of receipts.

Receipts vs. Users:
Issue: 117 (258 - 141) users in receipts have no corresponding user records, indicating missing user data or orphaned receipts.
Impact: User demographics or behavior analysis is incomplete for 45.3% of receipt users.

Items vs. Brands:
Issue: 568 distinct barcodes in items, only 15 in brands_cleaned; 1138 (1153 - 15) barcodes in brands not in items. Only 2.6% of items barcodes match brands, and 98.7% of brands barcodes are unused in items, showing a significant linkage gap.
Impact: Brand-level insights (e.g., top brands by sales) are severely limited.

Missing Barcode Data in items
Issue: 3851 (6941 - 3090) item records (55.5%) lack barcodes, preventing linkage to brands or product identification.
Impact: Over half of item-level data is unusable for brand or product analysis.
*/--------------------***************************************************************-------------------------------------


------------- SECOND: WRITE QUERIES THAT DIRECTLY ANSWER PREDETERMINED QUESTIONS FROM A BUSINESS STAKEHOLDER -------------
-- What are the top 5 brands by receipts scanned for most recent month?
-- Assumption:
-- Pretending that the maximum receipts.date_scanned represents today's date.
select d.name brand_name, count(distinct a.receipt_id)cnt_distinct_receipts
from receipts a
left join items c on a.receipt_id = c.receipt_id
left join brands_cleaned d on c.barcode = d.barcode
where 1=1
and a.date_scanned >= date_trunc('month', (select max(date_scanned) from receipts) - interval '1 month')
and a.date_scanned < date_trunc('month', (select max(date_scanned) from receipts))
group by 1
order by 2 desc
limit 5
;
-- results:
-- brand_name,  cnt_distinct_receipts
-- <null>,      444
-- Answer:
---The SQL query above answers the question; however, due to data deficiencies in the brands/barcode data, we are unable to break down the count of receipts by brand


-- How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
-- Assumption:
-- Pretending that the maximum receipts.date_scanned represents today's date.
select d.name brand_name, count(distinct a.receipt_id)cnt_distinct_receipts
from receipts a
left join items c on a.receipt_id = c.receipt_id
left join brands_cleaned d on c.barcode = d.barcode
where 1=1
and a.date_scanned >= date_trunc('month', (select max(date_scanned) from receipts) - interval '2 month')
and a.date_scanned < date_trunc('month', (select max(date_scanned) from receipts) - interval '1 month')
group by 1
order by 2 desc
limit 5
;
-- results:
-- brand_name,                    cnt_distinct_receipts
-- <null>,                          637
-- Swanson,                         11
-- Tostitos,                        11
-- Cracker Barrel Cheese,           10
-- Kraft,                           3
-- Answer:
---If we have complete brands/barcode data, the results from this query compared to the results from the previous query will allow us to analyze and compare the rankings between the two months.


-- When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
select rewards_receipt_status, avg(total_spent) average_spent from receipts group by 1 order by 1;
-- results:
-- rewards_receipt_status,  average_spent
-- FINISHED,                80.85430501930502
-- FLAGGED,                 180.4517391304348
-- PENDING,                 28.03244897959184
-- REJECTED,                23.326056338028184
-- SUBMITTED,               <null>
-- Answer:
---Grouping all the receipts by rewardsReceiptStatus, we observe that there is no status labeled as 'Accepted'.
-- If we assume that 'Finished' is equivalent to 'Accepted', the average spend for 'Finished' ($80.85) is higher than that for 'Rejected' ($23.33).


-- When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
select rewards_receipt_status, sum(purchased_item_count) total_items_purchased from receipts group by 1 order by 1;
-- results:
-- rewards_receipt_status,  total_items_purchased
-- FINISHED,                8184
-- FLAGGED,                 1014
-- PENDING,                 <null>
-- REJECTED,                173
-- SUBMITTED,               <null>
-- Answer:
---Grouping all the receipts by rewardsReceiptStatus, we observe that there is no status labeled as 'Accepted'.
-- If we assume that 'Finished' is equivalent to 'Accepted', the total number of items purchased for 'Finished' (8,184) is greater than that for 'Rejected' (173).


-- Which brand has the most spend among users who were created within the past 6 months?
-- Assumptions:
-- Pretending that the maximum users_dedup.created_date represents today's date.
-- Assuming that the sum of the final_price from the items table is equivalent to the total_spent value in the receipts table.
select d.name brand_name, sum(c.final_price) total_spent
from receipts a
inner join users_dedup b on a.user_id = b.user_id
                        and b.created_date >= date_trunc('month', (select max(created_date) from users_dedup) - interval '6 month')
                        and b.created_date < date_trunc('month', (select max(created_date) from users_dedup))
left join items c on a.receipt_id = c.receipt_id
left join brands_cleaned d on c.barcode = d.barcode
group by 1
order by 2 desc;
-- results:
-- brand_name,              total_spent
-- <null>,                  33035.910000000775
-- Cracker Barrel Cheese,   196.98000000000002
-- Tostitos,                80.65999999999998
-- Swanson,                 61.37999999999999
-- Cheetos,                 22
-- V8,                      13.49
-- Kettle Brand,            11.07
-- Pepperidge Farm,         9
-- Jell-O,                  4.99
-- Quaker,                  3.99
-- Grey Poupon,             3.29
-- Answer:
---Ignoring spending on unknown brands (NULL brand_name), the brand with the highest total spend among users created within the past six months is Cracker Barrel Cheese ($196.98).


-- Which brand has the most transactions among users who were created within the past 6 months?
-- Assumptions:
-- Pretending that the maximum users_dedup.created_date represents today's date.
-- Assuming that one transaction is equivalent to one receipt.
select d.name brand_name, count(distinct a.receipt_id) total_transctions, sum(c.final_price) total_spent
from receipts a
inner join users_dedup b on a.user_id = b.user_id
                        and b.created_date >= date_trunc('month', (select max(created_date) from users_dedup) - interval '6 month')
                        and b.created_date < date_trunc('month', (select max(created_date) from users_dedup))
left join items c on a.receipt_id = c.receipt_id
left join brands_cleaned d on c.barcode = d.barcode
group by 1
order by 2 desc, 3 desc;
-- results:
-- brand_name,              total_transctions,      total_spent
-- <null>,                  823,                    33035.91000000049
-- Tostitos,                11,                     80.66
-- Swanson,                 11,                     61.37999999999999
-- Kettle Brand,            3,                      11.07
-- Cracker Barrel Cheese,   2,                      196.98000000000002
-- Jell-O,                  2,                      4.99
-- Cheetos,                 1,                      22
-- V8,                      1,                      13.49
-- Pepperidge Farm,         1,                      9
-- Quaker,                  1,                      3.99
-- Grey Poupon,             1,                      3.29
-- Answer:
---Ignoring spending on unknown brands (NULL brand_name), the brands with the highest number of transactions among users created within the past six months are Tostitos and Swanson.
-- However, Tostitos has the higher total spend.

