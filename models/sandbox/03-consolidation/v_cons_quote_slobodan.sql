{{
  config(
    materialized='view',
    schema='DW_L03_CONSOLIDATION_DBT'
  )
}}


with
    cte_trn_quote as (

SELECT
    id AS quote_id,
    ws_2_quote_name_c AS quote_name,
    sbqq_opportunity_2_c AS opportunity_id,
    CAST(ws_2_project_start_date_c AS DATE) AS ws_2_project_start_date_c,
    CAST(initial_planning_end_date_c AS DATE) AS initial_planning_end_date_c,
    cast(ws_2_duration_months_c as integer) AS ws_2_duration_months_c,
    ws_2_duration_quarter_c AS ws_2_duration_quarter_c,
    ws_2_change_order_c AS ws_2_change_order_c,
    monthly_c AS monthly_c,
    quaterly_c AS quaterly_c,
    sbqq_primary_c AS sbqq_primary_c,
    probability_c AS probability,
    ws_2_status_c AS status,
    sbqq_type_c AS quote_type,
    CAST(MONTH(TO_DATE(ws_2_project_start_date_c)) AS integer) AS initial_planning_start_month,
   CAST(MONTH(TO_DATE(initial_planning_end_date_c)) AS integer) AS initial_planning_end_month,
    IFF(
        ws_2_project_start_date_c = initial_planning_end_date_c,
        CAST(MONTH(TO_DATE(ws_2_project_start_date_c)) AS VARCHAR),
        CAST(MONTH(ADD_MONTHS(TO_DATE(ws_2_project_start_date_c), 1)) AS VARCHAR)
    ) AS forecast_start_month,
    CAST(
        DATEADD(
            'month',
            1,
            DATE_TRUNC('month', CAST(ws_2_project_start_date_c AS DATE))
        ) AS DATE
    ) AS forecast_start_date,
    CAST(QUARTER(TO_DATE(initial_planning_end_date_c)) AS VARCHAR) AS initial_planning_end_quarter,
    CAST(QUARTER(TO_DATE(ws_2_project_start_date_c)) AS VARCHAR) AS initial_planning_start_quarter,
    IFF(
        ws_2_project_start_date_c = ws_2_project_start_date_c,
        CAST(QUARTER(TO_DATE(ws_2_project_start_date_c)) AS VARCHAR),
        CAST(
            QUARTER(ADD_MONTHS(TO_DATE(ws_2_project_start_date_c), 1)) AS VARCHAR
        )
    ) AS forecast_start_quarter,
    CAST(
        DATE_TRUNC('quarter', CAST(ws_2_project_start_date_c AS DATE)) AS DATE
    ) AS forecast_start_quarter_date
        

FROM
    {{ ref('snapshot_QUOTE_SLOBODAN') }}
    
    where DBT_VALID_TO is null

    )


select *  from cte_trn_quote
