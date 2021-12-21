select 
	mcm as old_mcm
	, tpa_mcm_id as new_mcm
	, outlet_identifier as new_outletid
    , lw.accountid
    , lw.outletid as old_outletid
    , lw.merchantname
    , address.business_add
    , datecreated
    , merchantcategory
    , businesscategory
    , city
    , state
    , lw.usercnt AS lwusercnt
    , lw.gtvrm AS lwgtv
    , gtv_user
    --, COALESCE(twa.gtvrm,0.0) AS twagtv
    --, lw.gtvrm - COALESCE(twa.gtvrm,0.0) AS wdgtv
    --, COALESCE(mtdty.gtvrm,0.0) AS mtdtygtv
    --, COALESCE(mtdly.gtvrm,0.0) AS mtdlygtv
    --, COALESCE(tmavg.gtvrm,0.0) AS tmavggtv
from
	(select 
		week
		, accountid
		, outletid
		, tpa_mcm_id
		, outlet_identifier
		, merchantname
		, usercnt
		, txncnt
		, gtvrm
		, round(gtvrm/txncnt, 2) as gtv_user
	from 
		(select 
			base.week
			, base.accountid
			, base.outletid
			, base.merchantname
			, base.tpa_mcm_id
			, base.outlet_identifier
			, count(distinct base.msisdn) as usercnt
			, count(distinct base.vtm_referenceid) as txncnt
			, sum(amountrm) as gtvrm
		from 			
			(select 
				sml.eap_merchantaccountid as accountid
				, sml.outlet_id AS outletid
				, sml.tpa_mcm_id
				, sml.outlet_identifier
				, msisdn
				, amountrm
				, vtm_referenceid 
				, vtm_type 
				, vtm_status 
				, week
			    , date
			    , merchantname 
			from (select 
					vtm_accountid
					, vtm_additionaldata_outletid
					, vtm_additionaldata_payermsisdn as msisdn
					, round(vtm_amount / 100.00,2) as amountrm
					, vtm_referenceid 
					, vtm_type 
					, vtm_status 
					, DATE_TRUNC('week',vtc_datecreated) AS week
				    , DATE_TRUNC('day',vtc_datecreated) AS date
				    , replace(upper(vtc_description), 'PAID ','') as merchantname 
				from aggregation_layer.payment_aggregate pa 
				where vtc_category = 'PAYMENT'
						and vtc_status = 'CAPTURED'
						and vtc_datecreated >= DATE_TRUNC('week',GETDATE() - interval '1 week') ) txn
			inner join (SELECT
							account_identifier
							, eap_merchantaccountid
							, outlet_identifier -- new outletid
							, outlet_id -- old outlet id
							, merchantaccountid
							, tpa_mcm_id
						FROM
							merchant_datamart.sparkle_merchantlist_lkp) sml
			ON txn.vtm_accountid = sml.merchantaccountid
				AND txn.vtm_additionaldata_outletid = sml.outlet_identifier) base
		group by 1,2,3,4,5,6)
	where usercnt <= 10) lw
inner join 
	(
		select 
			eap_mcm_id as mcm
			, eap_merchantaccountid as accountid
			, eap_outlet_id as outlet_id
			, merchant_category as merchantcategory
			, business_industry_level_1 as businesscategory
			, outlet_city as city
			, outlet_state as state
			, date(outlet_created_datetime) as datecreated
		from daas_datamart.dim_merchant_profile dmp 
		) mcm
	on lw.accountid = mcm.accountid and lw.outletid = mcm.outlet_id
inner join /*queries the address*/
	(
	select 
		new_mcm, new_outlet, old_mcm, old_outlet, merchant_name, accountid,
		(add1 || add2 || city || state || postcode) as business_add
	from 
		(select mcm_id as new_mcm
			, outlet_id as new_outlet
			, eap_mcm_id as old_mcm
			, eap_outlet_id as old_outlet
			, outlet_name as merchant_name
			, eap_merchantaccountid as accountid
			, initcap(business_address_line_1) + ', \n' as add1
			, case when business_address_line_2 != 'XXX'
				then initcap(business_address_line_2) + ', \n'
				else '' end as add2
			, case when business_city != 'XXX'
				then initcap(business_city) + ', '
				else '' end as city
			, case when business_state != 'XXX'
				then initcap(business_state) + ', '
				else '' end as state
			, case when business_postal_code != 'XXX'
				then business_postal_code 
				else '' end as postcode
		from daas_datamart.dim_merchant_profile dmp) 
	) address
	on mcm.accountid = address.accountid and mcm.outlet_id = address.old_outlet 
