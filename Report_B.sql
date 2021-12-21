select 
	date
	, mcm as old_mcm
	, base.accountid
	, base.outletid as old_outletid
	, tpa_mcm_id as new_mcm
	, outlet_identifier as new_outletid
	, merchantname
	, merchantcategory
	, businesscategory
	, city
	, state
	, customerid
	--, base.clean_msisdn
	, txncnt 
	, totalamountrm 
	, minamountrm 
	, maxamountrm 
	, avgamountrm 
	, mtdtxncnt
	, mtdtotalamountrm
	, date(datecreated) as datecreated
from 
	(/*this nest is to group everything together nicely*/
	 select
		 date
		 , accountid
		 , outletid
		 , merchantname
		 , outlet_identifier
		 , tpa_mcm_id
		 , customerid
		 , msisdn 
		 --, clean_msisdn
		 , count(vtm_referenceid) as txncnt
		 , SUM(amountrm) AS totalamountrm
	     , MIN(amountrm) AS minamountrm
	     , MAX(amountrm) AS maxamountrm
	     , AVG(amountrm) AS avgamountrm
     from
		(select 
			sml.eap_merchantaccountid as accountid
			, sml.outlet_id AS outletid
			, msisdn
			, sml.outlet_identifier
			, sml.tpa_mcm_id
			, customerid
			, amountrm
			, vtm_referenceid 
			, vtm_type 
			, vtm_status 
			, week
		    , date
		    , merchantname 
		    --, clean_msisdn
		from (select 
				vtm_accountid
				, vtm_additionaldata_outletid
				, vtm_additionaldata_payermsisdn as msisdn
				, round(vtm_amount / 100.00,2) as amountrm
				, customerid
				, vtm_referenceid 
				, vtm_type 
				, vtm_status 
				, DATE_TRUNC('week',vtc_datecreated + interval '8 hours') AS week
			    , DATE_TRUNC('day',vtc_datecreated + interval '8 hours') AS date
			    , replace(upper(vtc_description), 'PAID ','') as merchantname 
			from aggregation_layer.payment_aggregate pa 
			where vtc_category = 'PAYMENT'
					and vtc_status = 'CAPTURED'
					and vtc_datecreated >= DATE_TRUNC('day',GETDATE() - interval '7 days') ) txn
		inner join (SELECT
						account_identifier
						, eap_merchantaccountid
						, outlet_identifier
						, outlet_id
						, merchantaccountid
						, tpa_mcm_id
					FROM
						merchant_datamart.sparkle_merchantlist_lkp) sml
		/*this part is there a way to put if outlet_identifier like '%MCO%' then use outlet identifier
		 * else use eap_merchantaccountid*/
		ON txn.vtm_accountid = sml.merchantaccountid
			AND txn.vtm_additionaldata_outletid = sml.outlet_identifier 
		inner join (
			select
				id
				, merchantidentifier as mcm_id
				, accountmaincontactperson_contactno as hashed_msisdn
			from merchant_management.businessaccount 
			) dmpe
		on sml.account_identifier = dmpe.mcm_id 
			and txn.msisdn = dmpe.hashed_msisdn)
		group by 1,2,3,4,5,6,7,8) base
left join 
	(
		select 
			eap_mcm_id as mcm
			, eap_merchantaccountid as accountid
			, eap_outlet_id as outlet_id
			, merchant_category as merchantcategory
			, business_industry_level_1 as businesscategory
			, outlet_city as city
			, outlet_state as state
			, outlet_created_datetime as datecreated
		from daas_datamart.dim_merchant_profile dmp 
		) mcm
	on base.accountid = mcm.accountid and base.outletid = mcm.outlet_id
inner join (/*this is the monthly one*/
	select 
		sml.eap_merchantaccountid as accountid
		, sml.outlet_id as outletid
		, msisdn
		, count(vtm_referenceid) as mtdtxncnt
		, sum(amountrm) as mtdtotalamountrm
	From
		(select 
			vtm_accountid
			, vtm_additionaldata_outletid
			, vtm_additionaldata_payermsisdn as msisdn
			, round(vtm_amount / 100.00,2) as amountrm
			, customerid
			, vtm_referenceid 
			, vtm_type 
			, vtm_status 
			, DATE_TRUNC('month',vtc_datecreated + interval '8 hours') AS month
		    , DATE_TRUNC('day',vtc_datecreated + interval '8 hours') AS date
		    , replace(upper(vtc_description), 'PAID ','') as merchantname 
		from aggregation_layer.payment_aggregate pa 
		where vtc_category = 'PAYMENT'
				and vtc_status = 'CAPTURED'
				and vtc_datecreated >= DATE_TRUNC('day',GETDATE() - interval '30 days') ) txn
	inner join (SELECT
					account_identifier
					, eap_merchantaccountid
					, outlet_identifier
					, outlet_id
					, merchantaccountid
				FROM
					merchant_datamart.sparkle_merchantlist_lkp) sml
	ON txn.vtm_accountid = sml.merchantaccountid
		AND txn.vtm_additionaldata_outletid = sml.outlet_identifier 
	group by 1,2,3) vtmmtd
on base.accountid = vtmmtd.accountid
       AND base.outletid = vtmmtd.outletid
       AND base.msisdn = vtmmtd.msisdn
union all 
select 
	date
	, mcm as old_mcm
	, base.accountid
	, base.outletid as old_outletid
	, tpa_mcm_id as new_mcm
	, outlet_identifier as new_outletid
	, merchantname
	, merchantcategory
	, businesscategory
	, city
	, state
	, customerid
	--, base.clean_msisdn
	, txncnt 
	, totalamountrm 
	, minamountrm 
	, maxamountrm 
	, avgamountrm 
	, mtdtxncnt
	, mtdtotalamountrm
	, date(datecreated) as datecreated
from 
	(/*this nest is to group everything together nicely*/
	 select
		 date
		 , accountid
		 , outletid
		 , merchantname
		 , outlet_identifier
		 , tpa_mcm_id
		 , customerid
		 , msisdn 
		 --, clean_msisdn
		 , count(vtm_referenceid) as txncnt
		 , SUM(amountrm) AS totalamountrm
	     , MIN(amountrm) AS minamountrm
	     , MAX(amountrm) AS maxamountrm
	     , AVG(amountrm) AS avgamountrm
     from
		(select 
			sml.eap_merchantaccountid as accountid
			, sml.outlet_id AS outletid
			, msisdn
			, sml.outlet_identifier
			, sml.tpa_mcm_id
			, customerid
			, amountrm
			, vtm_referenceid 
			, vtm_type 
			, vtm_status 
			, week
		    , date
		    , merchantname 
		    --, clean_msisdn
		from (select 
				vtm_accountid
				, vtm_additionaldata_outletid
				, vtm_additionaldata_payermsisdn as msisdn
				, round(vtm_amount / 100.00,2) as amountrm
				, customerid
				, vtm_referenceid 
				, vtm_type 
				, vtm_status 
				, DATE_TRUNC('week',vtc_datecreated + interval '8 hours') AS week
			    , DATE_TRUNC('day',vtc_datecreated + interval '8 hours') AS date
			    , replace(upper(vtc_description), 'PAID ','') as merchantname 
			from aggregation_layer.payment_aggregate pa 
			where vtc_category = 'PAYMENT'
					and vtc_status = 'CAPTURED'
					and vtc_datecreated >= DATE_TRUNC('day',GETDATE() - interval '7 days') ) txn
		inner join (SELECT
						account_identifier
						, eap_merchantaccountid
						, outlet_identifier
						, outlet_id
						, merchantaccountid
						, tpa_mcm_id
					FROM
						merchant_datamart.sparkle_merchantlist_lkp) sml
		/*this part is there a way to put if outlet_identifier like '%MCO%' then use outlet identifier
		 * else use eap_merchantaccountid*/
		ON txn.vtm_accountid = sml.eap_merchantaccountid
			AND txn.vtm_additionaldata_outletid = sml.outlet_id 
		inner join (
			select
				id
				, merchantidentifier as mcm_id
				, accountmaincontactperson_contactno as hashed_msisdn
			from merchant_management.businessaccount 
			) dmpe
		on sml.account_identifier = dmpe.mcm_id 
			and txn.msisdn = dmpe.hashed_msisdn)
		group by 1,2,3,4,5,6,7,8) base
left join 
	(
		select 
			eap_mcm_id as mcm
			, eap_merchantaccountid as accountid
			, eap_outlet_id as outlet_id
			, merchant_category as merchantcategory
			, business_industry_level_1 as businesscategory
			, outlet_city as city
			, outlet_state as state
			, outlet_created_datetime as datecreated
		from daas_datamart.dim_merchant_profile dmp 
		) mcm
	on base.accountid = mcm.accountid and base.outletid = mcm.outlet_id
inner join (/*this is the monthly one*/
	select 
		sml.eap_merchantaccountid as accountid
		, sml.outlet_id as outletid
		, msisdn
		, count(vtm_referenceid) as mtdtxncnt
		, sum(amountrm) as mtdtotalamountrm
	From
		(select 
			vtm_accountid
			, vtm_additionaldata_outletid
			, vtm_additionaldata_payermsisdn as msisdn
			, round(vtm_amount / 100.00,2) as amountrm
			, customerid
			, vtm_referenceid 
			, vtm_type 
			, vtm_status 
			, DATE_TRUNC('month',vtc_datecreated + interval '8 hours') AS month
		    , DATE_TRUNC('day',vtc_datecreated + interval '8 hours') AS date
		    , replace(upper(vtc_description), 'PAID ','') as merchantname 
		from aggregation_layer.payment_aggregate pa 
		where vtc_category = 'PAYMENT'
				and vtc_status = 'CAPTURED'
				and vtc_datecreated >= DATE_TRUNC('day',GETDATE() - interval '30 days') ) txn
	inner join (SELECT
					account_identifier
					, eap_merchantaccountid
					, outlet_identifier
					, outlet_id
					, merchantaccountid
				FROM
					merchant_datamart.sparkle_merchantlist_lkp) sml
	ON txn.vtm_accountid = sml.eap_merchantaccountid
			AND txn.vtm_additionaldata_outletid = sml.outlet_id 
	group by 1,2,3) vtmmtd
on base.accountid = vtmmtd.accountid
       AND base.outletid = vtmmtd.outletid
       AND base.msisdn = vtmmtd.msisdn
order by mtdtotalamountrm desc
