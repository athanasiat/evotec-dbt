{{ config(materialized="view", schema="DW_L03_CONSOLIDATION_DBT") }}

with
    cte_trn_plan as (
        select
            id as plan_id,
            ws_2_quote_line_c as quoteline_id,
            number_c,
            CAST("WS_2_FTE_C" AS INTEGER) as fte,
            name as plan_name,
            currencyisocode,
            ws_2_month_number_c,
            ws_2_quarter_number_c,
            ws_2_quote_line_c,
            number_c as sequence_nr,
             IFF(CONTAINS(UPPER(NAME),'MONTH'),'Month',IFF(CONTAINS(UPPER(NAME),'QUARTER'),'Quarter','')) as plan_period

        from {{ ref("snapshot_WS_2_PLAN_AT") }}

        where DBT_VALID_TO is null
    )

select *
from cte_trn_plan
