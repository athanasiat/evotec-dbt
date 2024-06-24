{{ config(materialized="view", 
        schema="DW_L03_CONSOLIDATION_DBT")
 }}


with
    cte_trn_quoteline as (
        select
            sbqq_quote_c as quote_id,
            id as quoteline_id,
            ws_2_per_c,
            cast(ws_2_sales_fte_rate_c as integer) as sales_fte_rate,
            ws_2_unit_of_measure_c,
            cast(ws_2_unit_price_c as integer) as ws_2_unit_price_c,
            cast(ws_2_net_total_price_c as float) as ws_2_net_total_price_c,
            business_area_c

        from {{ ref("snapshot_QUOTELINE") }}

        where DBT_VALID_TO is null
    )



select * from cte_trn_quoteline
