create table trans_type_master (channel string, trans_type string);

insert into trans_type_master 
	values('ALAT','RECHARGE'),('ALAT','OTHER'),('POS','RECHARGE'),
		('POS','OTHER'),('ATM','RECHARGE'),('ATM','OTHER'),
		('USSD','RECHARGE'),('USSD','OTHER'),('ONLINE TRANSFER','RECHARGE'),
		('ONLINE TRANSFER','OTHER');

