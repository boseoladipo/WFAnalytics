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
    sum(case when to_date(trans_date) >= date_add(current_date,-14) then trns_amt else 0 end) over w1 sum_2wks,
    sum(case when to_date(trans_date) >= date_add(current_date,-30) then trns_amt else 0 end) over w1 sum_mon,
    sum(case when to_date(trans_date) >= date_add(current_date,-90) then trns_amt else 0 end) over w1 sum_qtr,
    sum(case when to_date(trans_date) >= date_add(current_date,-365) then trns_amt else 0 end) over w1 sum_year,
    count(case when dayofweek(trans_date) between 2 and 5 then trns_amt end) over w1 as cnt_wkday,
    count(case when dayofweek(trans_date) not between 2 and 5 then trns_amt end) over w1 as cnt_wkend,
	count(case when dayofmonth(trans_date) <= 6 or dayofmonth(trans_date) >=24 then trns_amt end) over w1 as cnt_riskmon,
	count(case when dayofmonth(trans_date) between 7 and 23 then trns_amt end) over w1 as cnt_normalmon,
	count(case when trns_amt between 0 and 499 then trns_amt end) over w1 as cnt_amt500,
	count(case when trns_amt between 500 and 4999 then trns_amt end) over w1 as cnt_amt5k,
	count(case when trns_amt between 5000 and 19999 then trns_amt end) over w1 as cnt_amt20k,
	count(case when trns_amt between 20000 and 99999 then trns_amt end) over w1 as cnt_amt100k,
	count(case when trns_amt >= 100000 then trns_amt end) over w1 as cnt_amtabove100k,
    count(case when hour(trans_date) between 2 and 5 then trns_amt end) over w1 as cnt_earlymorning,
    count(case when hour(trans_date) between 6 and 10 then trns_amt end) over w1 as cnt_morning,
    count(case when hour(trans_date) between 11 and 17 then trns_amt end) over w1 as cnt_afternoon,
    count(case when hour(trans_date) between 18 and 21 then trns_amt end) over w1 as cnt_evening,
    count(case when hour(trans_date) between 22 and 23 or hour(trans_date) between 0 and 1 then trns_amt end) over w1 as cnt_midnight,
    '2019-03-01 00:00:00' as last_transtime,
    '100' as last_transamt,
    'CA 100' as last_transprd,
	count(case when to_date(trans_date) = current_date then trns_amt else 0 end) over w1 cnt_today,
	(count(trns_amt) over w1)/count(distinct to_date(trans_date)) over w1 avg_daily_cnt,
	avg(case when to_date(trans_date) >= date_add(current_date,-365) then trns_amt else 0 end) over w1 avg_year,
	count(case when trans_type = 'RECHARGE' then trns_amt end) over w1 as cnt_recharge,
	count(case when trans_type = 'NAIRABAT' then trns_amt end) over w1 as cnt_nairabat,
	count(case when trans_type = 'PAY_BILL' then trns_amt end) over w1 as cnt_paybill,
	count(case when trans_type = 'CUSTFUNDTRANSF' then trns_amt end) over w1 as cnt_custfundtransf,
	count(case when trans_type = 'FUND_TRNS_OTHR' then trns_amt end) over w1 as cnt_fundtrnsothr,
	count(case when trans_type = 'INIFUNDTRANSF' then trns_amt end) over w1 as cnt_inifundtransf
from (select * from fraud.fund_transfer_tbl where trans_type in (select distinct trans_type from trans_type_master) ) a
-- where accountno in ('0109512191', '0109609275','0009420026')
window w1 as (partition by accountno, channel, trans_type)
;


drop table join_table;
create temporary table join_table as 
select a.acct_no, b.first_name, b.middle_name, b.last_name, b.gender, b.dob, b.m_prd_code, a.acct_type 
	from (select * from mob_acct_data where acct_no is not null) a 
	left join mob_customer_master b on a.cust_id = b.id;



set hive.auto.convert.join=true;

drop table account_trantype;

create temporary table account_trantype as 
select /* mapjoin(b) */ *, concat_ws('.', a.acct_no , b.channel, b.trans_type) join_key from 
	join_table a,
	trans_type_master b;




drop table aggregate_master;


create table aggregate_master as 

select /* mapjoin(b) */ a.join_key, a.acct_no, a.channel, a.trans_type, nvl(a.first_name,0) first_name, nvl(a.middle_name,0) middle_name, nvl(a.m_prd_code,0) m_prd_code,
  0 as debit_card_used, nvl(a.dob,0) dob,
	nvl(a.last_name,0) last_name, nvl(a.gender,0) gender, nvl(a.acct_type,0) acct_type, nvl(a.balance,0),
	nvl(b.sum_1d,0) sum_1d, nvl(b.sum_2d,0) sum_2d, nvl(b.sum_3d,0) sum_3d, nvl(b.sum_4d,0) sum_4d, nvl(b.sum_5d,0) sum_5d, nvl(b.sum_6d,0) sum_6d, nvl(b.sum_7d,0) sum_7d,
	nvl(b.sum_2wks,0) sum_2wks, 
	nvl(b.sum_mon,0) sum_mon, nvl(b.sum_qtr,0) sum_qtr, nvl(b.sum_year,0) sum_year,
    nvl(b.cnt_wkday/(b.cnt_wkday+b.cnt_wkend),0) frq_wkday, 
    nvl(b.cnt_wkend/(b.cnt_wkday+b.cnt_wkend),0) frq_wkend, 
	nvl(b.cnt_riskmon/(b.cnt_riskmon+b.cnt_normalmon),0) frq_riskmon, 
    nvl(b.cnt_normalmon/(b.cnt_riskmon+b.cnt_normalmon),0) frq_normalmon,
	nvl((b.cnt_amt500)/(cnt_amt500+cnt_amt5k+cnt_amt20k+cnt_amt100k+cnt_amtabove100k),0) frq_amt500,
	nvl((b.cnt_amt5k)/(cnt_amt500+cnt_amt5k+cnt_amt20k+cnt_amt100k+cnt_amtabove100k),0) frq_amt5k,
	nvl((b.cnt_amt20k)/(cnt_amt500+cnt_amt5k+cnt_amt20k+cnt_amt100k+cnt_amtabove100k),0) frq_amt20k,
	nvl((b.cnt_amt100k)/(cnt_amt500+cnt_amt5k+cnt_amt20k+cnt_amt100k+cnt_amtabove100k),0) frq_amt100k,
	nvl((b.cnt_amtabove100k)/(cnt_amt500+cnt_amt5k+cnt_amt20k+cnt_amt100k+cnt_amtabove100k),0) frq_amtabove100k,
    nvl(b.cnt_earlymorning/(b.cnt_earlymorning+b.cnt_morning+b.cnt_afternoon+b.cnt_evening+b.cnt_midnight),0) frq_earlymorning,
    nvl(b.cnt_morning/(b.cnt_earlymorning+b .cnt_morning+b.cnt_afternoon+b.cnt_evening+b.cnt_midnight),0) frq_morning,
    nvl(b.cnt_afternoon/(b.cnt_earlymorning+b.cnt_morning+b.cnt_afternoon+b.cnt_evening+b.cnt_midnight),0) frq_afternoon,
    nvl(b.cnt_evening/(b.cnt_earlymorning+b.cnt_morning+b.cnt_afternoon+b.cnt_evening+b.cnt_midnight),0) frq_evening,
    nvl(b.cnt_midnight/(b.cnt_earlymorning+b.cnt_morning+b.cnt_afternoon+b.cnt_evening+b.cnt_midnight),0) frq_midnight,
	nvl(b.cnt_recharge/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_recharge,
	nvl(b.cnt_nairabat/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_nairabat,
	nvl(b.cnt_paybill/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_paybill,
	nvl(b.cnt_custfundtransf/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_custfundtransf,
	nvl(b.cnt_fundtrnsothr/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_fundtrnsothr,
	nvl(b.cnt_inifundtransf/(b.cnt_recharge+b.cnt_nairabat+b.cnt_paybill+b.cnt_custfundtransf+b.cnt_fundtrnsothr+b.cnt_inifundtransf),0) frq_inifundtransf,
    nvl(b.last_transtime,0) last_transtime,
    nvl(b.last_transamt,0) last_transamt,
    nvl(b.last_transprd,0) last_transprd,
	nvl(b.cnt_today,0) cnt_today,
	nvl(b.avg_daily_cnt,0) avg_daily_cnt,
	nvl(b.avg_year,0) avg_year
from account_trantype a
left join init_agg b on a.join_key = b.join_key;