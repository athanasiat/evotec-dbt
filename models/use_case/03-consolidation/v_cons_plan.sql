{{ config(materialized="view", schema="DW_L03_CONSOLIDATION__DBT") }}

with
    cte_trn_plan as (
        select
            id as plan_id,
            ws_2_quote_line_c as quoteline_id,
            number_c,
            cast("WS_2_FTE_C" as integer) as fte,
            name as plan_name,
            currencyisocode,
            ws_2_month_number_c,
            ws_2_quarter_number_c,
            ws_2_quote_line_c,
            number_c as sequence_nr,
             IFF(CONTAINS(UPPER(NAME),'MONTH'),'Month',IFF(CONTAINS(UPPER(NAME),'QUARTER'),'Quarter','')) as plan_period

        from {{ ref("snapshot_WS_2_PLAN") }}
    )

select *
from cte_trn_plan
