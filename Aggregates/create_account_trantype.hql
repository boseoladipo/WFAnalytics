create table account_trantype as 
select /* mapjoin(b) */ acid, acct_num, foracid, acct_name, cif_id, cust_id, schm_code, acct_ownership
 concat_ws('.', a.acid , b.channel, b.trans_type) join_key from 
	account_master_tbl a,
	trans_type_master b;
