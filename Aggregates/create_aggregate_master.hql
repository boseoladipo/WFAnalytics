use fraud;

drop table init_agg;
create temporary table init_agg as
select 
distinct
accountno, channel, trans_type, concat_ws('.', accountno , channel, trans_type) join_key,
sum(case when to_date(trans_date) >= date_add(current_date,-1) then trns_amt else 0 end) over w1 sum_1d,
sum(case when to_date(trans_date) >= date_add(current_date,-2) then trns_amt else 0 end) over w1 sum_2d,
sum(case when to_date(trans_date) >= date_add(current_date,-3) then trns_amt else 0 end) over w1 sum_3d,
sum(case when to_date(trans_date) >= date_add(current_date,-4) then trns_amt else 0 end) over w1 sum_4d,
sum(case when to_date(trans_date) >= date_add(current_date,-5) then trns_amt else 0 end) over w1 sum_5d,
sum(case when to_date(trans_date) >= date_add(current_date,-6) then trns_amt else 0 end) over w1 sum_6d,
sum(case when to_date(trans_date) >= date_add(current_date,-7) then trns_amt else 0 end) over w1 sum_7d,

sum(case when to_date(trans_date) >= date_add(current_date,-1) then trns_amt else 0 end) over w4 sum_1d_channel, 
sum(case when to_date(trans_date) >= date_add(current_date,-2) then trns_amt else 0 end) over w4 sum_2d_channel,


'2019-03-01 00:00:00' as last_transtime,
'100' as last_transamt,
'CA 100' as last_transprd,

sum(case when to_date(trans_date) = current_date then trns_amt else 0 end) over w1 sum_today,
sum(case when to_date(trans_date) = current_date then trns_amt else 0 end) over w4 sum_today_channel,
'0' as cnt_today

from (select * from fraud.fund_transfer_tbl where responsecode = '00' and  trans_type in (select distinct trans_type from fraud.trans_type_master) ) a
window w1 as (partition by accountno, channel, trans_type),
w4 as (partition by accountno, channel) 
;

drop table join_table;
create temporary table join_table as 
select a.cust_id, b.customer_code, trim(a.acct_no) as acct_no, b.first_name, b.middle_name, b.last_name, b.gender, b.dob, b.m_prd_code, a.acct_type,
b.user_id, a.account_status, a.prim_account
from (select * from mob_acct_data_view where acct_no is not null and acct_no != 'null' and max_dt = date_created and entry_no = 1) a 
left join mob_customer_master b on a.cust_id = b.id;



set hive.auto.convert.join=true;

drop table account_trantype;

create temporary table account_trantype as 
select /* mapjoin(b) */ *, concat_ws('.', a.acct_no , b.channel, b.trans_type) join_key from 
join_table a,
trans_type_master b;




drop table aggregate_master;
create table aggregate_master as 

select /* mapjoin(b) */ a.join_key, a.cust_id, a.customer_code,
nvl(a.first_name,0) first_name,
nvl(a.middle_name,0) middle_name,
nvl(a.last_name,0) last_name,
nvl(a.gender,0) gender,
nvl(a.dob,0) dob,
nvl(a.acct_type,0) acct_type,
nvl(a.account_status,0) account_status,
nvl(a.user_id, 0) user_id,
nvl(a.prim_account,0) prim_account,
nvl(a.m_prd_code,0) m_prd_code,
0 as debit_card_used,
a.acct_no as accountno, 
a.channel,
a.trans_type, 
nvl(b.sum_today,0) sum_today,
nvl(b.sum_today_channel,0) sum_today_channel,
nvl(b.sum_1d,0) sum_1d,
nvl(b.sum_1d_channel,0) sum_1d_channel, 
nvl(b.sum_2d,0) sum_2d,
nvl(b.sum_2d_channel,0) sum_2d_channel,
nvl(b.sum_3d,0) sum_3d,
nvl(b.sum_4d,0) sum_4d,
nvl(b.sum_5d,0) sum_5d, 
nvl(b.sum_6d,0) sum_6d, 
nvl(b.sum_7d,0) sum_7d,
nvl(b.last_transtime,0) last_transtime,
nvl(b.last_transamt,0) last_transamt,
nvl(b.last_transprd,0) last_transprd,
b.cnt_today,
unix_timestamp(current_timestamp) as modified_date,
unix_timestamp(current_timestamp) as modified_date_channel

from account_trantype a
left join init_agg b on a.join_key = b.join_key
order by join_key;
