{% snapshot snapshot_WS_2_PLAN_SYED%}


{{
    config(
        target_schema='DW_L01_STG_ONE_CRM_DBT',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED'
     )
}}


select * from {{source ('DW_L01_STG_ONE_CRM_DBT','STG_WS_2_PLAN')}} 


{% endsnapshot %} 
EVOTEC_DB_DEV.DW_L01_STG_ONE_CRM_DBT.STG_WS_PLAN