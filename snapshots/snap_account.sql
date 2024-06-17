{% snapshot snap_Account %}


{{


    config(
        target_schema='DW_L02_PSH_ONE_CRM',
        unique_key='ID',
        strategy='timestamp',
        updated_at='_MODIFIED',
     )


}}


select * from {{source ('SALESFORCE','ACCOUNT')}}


{% endsnapshot %} 