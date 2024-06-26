{% snapshot snapshot_QUOTELINE_PLAN_KC %}
{{
    config(
        target_schema='DW_L02_PSH_ONE_CRM_DBT',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED'
     )
}}


select * from {{source ('SALESFORCE','STG_WS_2_PLAN')}} 

{% endsnapshot %}