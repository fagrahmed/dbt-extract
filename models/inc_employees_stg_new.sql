
{{ config(
    materialized='incremental',
    unique_key= ['employee_id', 'employee_mobile'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_employees_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists =stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

{% if table_exists %}

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

FROM {{ source('dbt-dimensions', 'inc_employees_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'inc_employees_dimension') }} dim on stg.employee_id = dim.employee_id
WHERE dim.employee_id is null

{% else %}

SELECT
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,    
    stg.clientid,
    stg.employee_mobile,
    stg.employee_id,
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

FROM {{ source('dbt-dimensions', 'inc_employees_stg') }} stg

{% endif %}