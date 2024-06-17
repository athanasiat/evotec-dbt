with
    cte_quote_clean as (
        select
            id as quote_id,
            sbqq_account_c as account_id,
            ws_2_project_start_date_c as project_start_date,
            initial_planning_end_date_c project_end_date
        from {{ ref("snap_QUOTE") }}
    )

select
    quote_id,  
    account_id,
    project_start_date,
    project_end_date
from cte_quote_clean
