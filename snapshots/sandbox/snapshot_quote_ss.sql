{% snapshot snapshot_QUOTE_SS %}


{{
    config(
        target_schema='DW_L02_PSH_ONE_CRM_DBT',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED'
     )
}}


select *  from {{source ('SALESFORCE','STG_QUOTE')}} 


{% endsnapshot %} 