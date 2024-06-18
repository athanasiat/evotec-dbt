{% snapshot snap_ACCOUNT_DEMO %}

    {{
        config(
            target_schema="DW_L02_PSH_ONE_CRM",
            unique_key="ID",
            strategy="timestamp",
            updated_at="_MODIFIED",
        )
    }}

    select id, billingcountrycode, _modified
    from {{ source("SALESFORCE", "ACCOUNT_DEMO") }}

{% endsnapshot %}
