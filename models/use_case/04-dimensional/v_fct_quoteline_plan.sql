{{ config(materialized="view", schema="DW_L04_DIMENSIONAL_DBT") }}



/* read all neccessary tables from 03-CONSOLIDATION LAYER
*/
with
    cte_quoteline as (select * from {{ ref("v_cons_quoteline") }}),
    cte_plan as (select * from {{ ref("v_cons_plan") }}),
    cte_quote as (select * from {{ ref("v_cons_quote") }}),



-- join the cte_quoteline , cte_plan , cte_quote tables for FTE
    cte_fte_joins as (
        select
            cte_plan.*,
            cte_quoteline.ws_2_per_c as ws_2_per_c,
            cte_quoteline.sales_fte_rate as sales_fte_rate,
            cte_quoteline.ws_2_unit_of_measure_c as ws_2_unit_of_measure_c,
            cte_quoteline.ws_2_unit_price_c as ws_2_unit_price_c,
            cte_quoteline.ws_2_net_total_price_c as ws_2_net_total_price_c,
            cte_quoteline.business_area_c as business_area,
            cte_quote.quote_id as quote_id,
            cte_quote.quote_name as quote_name,
            cte_quote.opportunity_id as opportunity_id,
            cte_quote.ws_2_project_start_date_c as ws_2_project_start_date_c,
            cte_quote.initial_planning_end_date_c as initial_planning_end_date_c,
            cte_quote.initial_planning_start_month as initial_planning_start_month,
            cte_quote.initial_planning_end_month as initial_planning_end_month,
            cte_quote.forecast_start_month as forecast_start_month,
            cte_quote.forecast_start_date as forecast_start_date,
            cte_quote.initial_planning_start_quarter as initial_planning_start_quarter,
            cte_quote.initial_planning_end_quarter as initial_planning_end_quarter,
            cte_quote.forecast_start_quarter as forecast_start_quarter,
            cte_quote.forecast_start_quarter_date as forecast_start_quarter_date,
            cte_quote.ws_2_duration_months_c as ws_2_duration_months_c,
            cte_quote.monthly_c as monthly_c,
            cte_quote.probability as probability,
            cte_quote.ws_2_change_order_c as ws_2_change_order_c,
            cte_quote.sbqq_primary_c as sbqq_primary_c,
            cte_quote.status as status
        from cte_plan
        left outer join
            cte_quoteline on cte_plan.quoteline_id = cte_quoteline.quoteline_id
        left outer join cte_quote on cte_quoteline.quote_id = cte_quote.quote_id

    ),


--generate months between for FFS
expanded_months_duration AS (
    SELECT 
        ql.QUOTELINE_id as QUOTELINE_id ,  
        q.WS_2_DURATION_MONTHS_C as WS_2_DURATION_MONTHS_C,
        1 AS SEQUENCE_NR
    FROM 
        cte_quoteline ql
    JOIN  cte_quote q
    ON ql.QUOTE_id = q.QUOTE_id
    UNION ALL
        SELECT 
        emd.QUOTEline_id,
        emd.WS_2_DURATION_MONTHS_C,
        emd.SEQUENCE_NR + 1 AS SEQUENCE_NR
    FROM 
        expanded_months_duration emd
        WHERE 
    emd.SEQUENCE_NR < EMD.WS_2_DURATION_MONTHS_C
),

-- join the cte_quoteline , cte_quote tables, and  expanded_months_duration for FFS
    cte_ffs_joins as (
         select cte_quote.*,
         cte_quoteline.QUOTELINE_ID	AS QUOTELINE_ID,
         cte_quoteline.WS_2_PER_C	AS WS_2_PER_C,
         cte_quoteline.SALES_FTE_RATE	AS SALES_FTE_RATE,
         cte_quoteline.WS_2_UNIT_OF_MEASURE_C	AS WS_2_UNIT_OF_MEASURE_C,
         cte_quoteline.WS_2_UNIT_PRICE_C	AS WS_2_UNIT_PRICE_C,
         cte_quoteline.WS_2_NET_TOTAL_PRICE_C	AS WS_2_NET_TOTAL_PRICE_C,
         cte_quoteline.BUSINESS_AREA_C	AS BUSINESS_AREA,
         cast(expanded_months_duration.SEQUENCE_NR as number) AS  SEQUENCE_NR       
        from cte_quoteline
        left outer join cte_quote on cte_quoteline.quote_id = cte_quote.quote_id
        left outer join expanded_months_duration  on cte_quoteline.QUOTELINE_id = expanded_months_duration.QUOTELINE_id
        )
        
        ,



    /* generate FTE_Monthly */
    cte_fte_monthly as (
        select
            PLAN_ID	as PLAN_ID,
            PLAN_NAME as 	PLAN_NAME,
            QUOTELINE_ID	as QUOTELINE_ID,
            QUOTE_ID	as QUOTE_ID,
            QUOTE_NAME	as QUOTE_NAME,
            OPPORTUNITY_ID	as OPPORTUNITY_ID,
            WS_2_PROJECT_START_DATE_C as	WS_2_PROJECT_START_DATE_C,
            INITIAL_PLANNING_END_DATE_C	as INITIAL_PLANNING_END_DATE_C,
            INITIAL_PLANNING_START_MONTH as 	INITIAL_PLANNING_START_MONTH,
            INITIAL_PLANNING_END_MONTH as	INITIAL_PLANNING_END_MONTH,
            INITIAL_PLANNING_START_QUARTER	as INITIAL_PLANNING_START_QUARTER,
            INITIAL_PLANNING_END_QUARTER	as INITIAL_PLANNING_END_QUARTER,
            FORECAST_START_DATE	as FORECAST_START_DATE,
            FORECAST_START_MONTH	as FORECAST_START_MONTH,
            FORECAST_START_QUARTER_DATE	as FORECAST_START_QUARTER_DATE,
            FORECAST_START_QUARTER	as FORECAST_START_QUARTER,
            cast(dateadd('month', "SEQUENCE_NR" - 1, "FORECAST_START_DATE") as date) as MONTHS,
            WS_2_DURATION_MONTHS_C as	WS_2_DURATION_MONTHS_C,
            MONTHLY_C	as MONTHLY_C,
            PROBABILITY	as PROBABILITY,
            WS_2_CHANGE_ORDER_C	as WS_2_CHANGE_ORDER_C,
            SBQQ_PRIMARY_C	as SBQQ_PRIMARY_C,
            SEQUENCE_NR	as SEQUENCE_NR,
            FTE	as FTE,
            PLAN_PERIOD	as PLAN_PERIOD,
            WS_2_PER_C	as SALES_FTE_RATE,
            WS_2_UNIT_OF_MEASURE_C	as WS_2_UNIT_OF_MEASURE_C,
            WS_2_UNIT_PRICE_C	as WS_2_UNIT_PRICE_C,
            WS_2_NET_TOTAL_PRICE_C	as WS_2_NET_TOTAL_PRICE_C,
            cast(
                case
                    when "SALES_FTE_RATE" is null
                    then "FTE" * "WS_2_UNIT_PRICE_C"
                    when "WS_2_PER_C" = 'per month'
                    then "FTE" * "SALES_FTE_RATE"
                    when "WS_2_PER_C" = 'per year'
                    then "FTE" * ("SALES_FTE_RATE" / 12)
                    else "FTE" * "WS_2_UNIT_PRICE_C"
                end as float
            ) as REVENUE,
            'FTE monthly' as FLAG_CALC,
            BUSINESS_AREA	as BUSINESS_AREA,
            STATUS	as STATUS                       
        from cte_fte_joins
        where not (quote_id is null) and plan_period = 'Month'
    ),



--generate FTE_Quarterly
    cte_fte_quarterly as (
        select
            "PLAN_ID" AS PLAN_ID,
           "PLAN_NAME" as	PLAN_NAME,
            QUOTELINE_ID	as QUOTELINE_ID,
            QUOTE_ID	as QUOTE_ID,
            QUOTE_NAME	as QUOTE_NAME,
            OPPORTUNITY_ID	as OPPORTUNITY_ID,
            WS_2_PROJECT_START_DATE_C as	WS_2_PROJECT_START_DATE_C,
            INITIAL_PLANNING_END_DATE_C	as INITIAL_PLANNING_END_DATE_C,
            INITIAL_PLANNING_START_MONTH as 	INITIAL_PLANNING_START_MONTH,
            INITIAL_PLANNING_END_MONTH as	INITIAL_PLANNING_END_MONTH,
            INITIAL_PLANNING_START_QUARTER	as INITIAL_PLANNING_START_QUARTER,
            INITIAL_PLANNING_END_QUARTER	as INITIAL_PLANNING_END_QUARTER,
            FORECAST_START_DATE	as FORECAST_START_DATE,
            FORECAST_START_MONTH	as FORECAST_START_MONTH,
            FORECAST_START_QUARTER_DATE	as FORECAST_START_QUARTER_DATE,
            FORECAST_START_QUARTER	as FORECAST_START_QUARTER,
            cast(dateadd('quarter', "SEQUENCE_NR" - 1, "FORECAST_START_DATE") as date) as MONTHS,
            WS_2_DURATION_MONTHS_C as	WS_2_DURATION_MONTHS_C,
            MONTHLY_C	as MONTHLY_C,
            PROBABILITY	as PROBABILITY,
            WS_2_CHANGE_ORDER_C	as WS_2_CHANGE_ORDER_C,
            SBQQ_PRIMARY_C	as SBQQ_PRIMARY_C,
            SEQUENCE_NR	as SEQUENCE_NR,
            FTE	as FTE,
            PLAN_PERIOD	as PLAN_PERIOD,
            WS_2_PER_C	as SALES_FTE_RATE,
            WS_2_UNIT_OF_MEASURE_C	as WS_2_UNIT_OF_MEASURE_C,
            WS_2_UNIT_PRICE_C	as WS_2_UNIT_PRICE_C,
            WS_2_NET_TOTAL_PRICE_C	as WS_2_NET_TOTAL_PRICE_C,
            cast(
                case
                    when "SALES_FTE_RATE" is null
                    then "FTE" * "WS_2_UNIT_PRICE_C"
                    when "WS_2_PER_C" = 'per month'
                    then "FTE" * "SALES_FTE_RATE"
                    when "WS_2_PER_C" = 'per year'
                    then "FTE" * ("SALES_FTE_RATE" / 12)
                    else "FTE" * "WS_2_UNIT_PRICE_C"
                end as float
            ) REVENUE,
            'FTE quarterly' as FLAG_CALC,
            BUSINESS_AREA	as BUSINESS_AREA,
            STATUS	as STATUS          
        from cte_fte_joins
        where not (quote_id is null) and plan_period = 'Quarter'
    ),


--generate FFS
 cte_ffs as (
        select
            'Unknown'  AS "PLAN_ID", 
            'Unknown' AS "PLAN_NAME",
            QUOTELINE_ID	as QUOTELINE_ID,
            QUOTE_ID	as QUOTE_ID,
            QUOTE_NAME	as QUOTE_NAME,
            OPPORTUNITY_ID	as OPPORTUNITY_ID,
            WS_2_PROJECT_START_DATE_C as	WS_2_PROJECT_START_DATE_C,
            INITIAL_PLANNING_END_DATE_C	as INITIAL_PLANNING_END_DATE_C,
            INITIAL_PLANNING_START_MONTH as 	INITIAL_PLANNING_START_MONTH,
            INITIAL_PLANNING_END_MONTH as	INITIAL_PLANNING_END_MONTH,
            INITIAL_PLANNING_START_QUARTER	as INITIAL_PLANNING_START_QUARTER,
            INITIAL_PLANNING_END_QUARTER	as INITIAL_PLANNING_END_QUARTER,
            FORECAST_START_DATE	as FORECAST_START_DATE,
            FORECAST_START_MONTH	as FORECAST_START_MONTH,
            FORECAST_START_QUARTER_DATE	as FORECAST_START_QUARTER_DATE,
            FORECAST_START_QUARTER	as FORECAST_START_QUARTER,
            cast(dateadd('month', "SEQUENCE_NR" - 1, "FORECAST_START_DATE") as date) as MONTHS,
            WS_2_DURATION_MONTHS_C as	WS_2_DURATION_MONTHS_C,
            MONTHLY_C	as MONTHLY_C,
            PROBABILITY	as PROBABILITY,
            WS_2_CHANGE_ORDER_C	as WS_2_CHANGE_ORDER_C,
            SBQQ_PRIMARY_C	as SBQQ_PRIMARY_C,
            SEQUENCE_NR	as SEQUENCE_NR,
            0 AS "FTE", 
            'Fee-for-service' as PLAN_PERIOD,
            WS_2_PER_C	as SALES_FTE_RATE,
            WS_2_UNIT_OF_MEASURE_C	as WS_2_UNIT_OF_MEASURE_C,
            WS_2_UNIT_PRICE_C	as WS_2_UNIT_PRICE_C,
            WS_2_NET_TOTAL_PRICE_C	as WS_2_NET_TOTAL_PRICE_C,
            cast("WS_2_NET_TOTAL_PRICE_C"/ NULLIF("WS_2_DURATION_MONTHS_C",0) as float) REVENUE,
            'Fee-for-service' as FLAG_CALC,
            BUSINESS_AREA	as BUSINESS_AREA,
            STATUS	as STATUS  
        from cte_ffs_joins
        where (NOT("WS_2_UNIT_OF_MEASURE_C" = 'FTEs')  AND NOT("WS_2_UNIT_OF_MEASURE_C" = ''))
    )

    


-- UNION FTE + FFS
select * from cte_fte_monthly
union
select * from cte_fte_quarterly
union 
select * from cte_ffs