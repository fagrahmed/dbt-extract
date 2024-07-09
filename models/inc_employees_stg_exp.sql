
{{ config(
    materialized='incremental',
    unique_key= ['employee_id', 'employee_mobile'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    stg.clientid,
    stg.employee_mobile,
    stg.employeeid,
    stg.hash_column,
    stg.employee_status,
    stg.employee_createdat_local,
    stg.employee_modifiedat_local,
    stg.employee_deletedat_local,
    stg.utc,
    stg.employee_salarytype,
    stg.tookfirstsalary,
    stg.iseligibleforclaimrequest,
    stg.iseligibleforadvancerequest,
    stg.advancerequestrequiresapproval,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours')

FROM {{ source('dbt-dimensions', 'inc_employees_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'inc_employees_dimension')}} final
    ON stg.employeeid = final.employeeid 
WHERE stg.loaddate > final.loaddate AND final.hash_column != stg.hash_column 

{% else %}

SELECT 
    stg.id,  
    stg.operation,
    stg.currentflag,
    stg.expdate,    
    stg.clientid,
    stg.employee_mobile,
    stg.employeeid,
    stg.hash_column,
    stg.employee_status,
    stg.employee_createdat_local,
    stg.employee_modifiedat_local,
    stg.employee_deletedat_local,
    stg.utc,
    stg.employee_salarytype,
    stg.tookfirstsalary,
    stg.iseligibleforclaimrequest,
    stg.iseligibleforadvancerequest,
    stg.advancerequestrequiresapproval,
    stg.loaddate

FROM {{ ref('inc_employees_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz 

{% endif %}