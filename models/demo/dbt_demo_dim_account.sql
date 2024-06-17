with
    cte_account_clean as (
        select
            id, billingcountrycode as country_code, min(_modified) as dts
        from {{ ref("snap_ACCOUNT") }}
        group by 1, 2
    )

select id, country_code, dts
from cte_account_clean
