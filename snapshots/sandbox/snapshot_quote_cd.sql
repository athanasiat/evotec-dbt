{% snapshot snapshot_QUOTE_CD %}


{{
    config(
        target_schema='DW_L01_STG_ONE_CRM_DBT',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED'
     )
}}


select *  from {{source ('SALESFORCE','STG_QUOTE')}} 


{% endsnapshot %} 