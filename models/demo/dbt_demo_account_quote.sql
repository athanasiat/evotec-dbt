with
    cte_join as (
        select *
        from {{ ref("dbt_demo_dim_quote") }} as q
        left join {{ ref("dbt_demo_dim_account") }} as acc on q.account_id = acc.id
    )

select *
from cte_join
