{% snapshot snapshot_QUOTE %}


{{
    config(
        target_schema='DW_L02_PSH_ONE_CRM_DBT',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED'
     )
}}


select *  from {{source ('USE_CASE_SALESFORCE','PSH_QUOTE_OLD')}} 


{% endsnapshot %} 