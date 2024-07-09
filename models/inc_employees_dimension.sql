

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='append_new_columns'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

WITH upd_exp_rec AS (

    SELECT
        id,
        operation,
        currentflag,
        expdate,
        clientid,
        employee_mobile,
        employeeid,
        hash_column,
        employee_status,
        employee_createdat_local,
        employee_modifiedat_local,
        employee_deletedat_local,
        utc,
        employee_salarytype,
        tookfirstsalary,
        iseligibleforclaimrequest,
        iseligibleforadvancerequest,
        advancerequestrequiresapproval,
        loaddate

    FROM {{ ref("inc_employees_stg_update") }}

    UNION ALL

    SELECT
        id,
        operation,
        currentflag,
        expdate,
        clientid,
        employee_mobile,
        employeeid,
        hash_column,
        employee_status,
        employee_createdat_local,
        employee_modifiedat_local,
        employee_deletedat_local,
        utc,
        employee_salarytype,
        tookfirstsalary,
        iseligibleforclaimrequest,
        iseligibleforadvancerequest,
        advancerequestrequiresapproval,
        loaddate
    
    FROM {{ ref("inc_employees_stg_exp") }}

)

{% if table_exists %}

, remove_old_from_dim AS (
    SELECT
        old_rec.id,
        old_rec.operation,
        old_rec.currentflag,
        old_rec.expdate,
        old_rec.clientid,
        old_rec.employee_mobile,
        old_rec.employeeid,
        old_rec.hash_column,
        old_rec.employee_status,
        old_rec.employee_createdat_local,
        old_rec.employee_modifiedat_local,
        old_rec.employee_deletedat_local,
        old_rec.utc,
        old_rec.employee_salarytype,
        old_rec.tookfirstsalary,
        old_rec.iseligibleforclaimrequest,
        old_rec.iseligibleforadvancerequest,
        old_rec.advancerequestrequiresapproval,
        old_rec.loaddate
    
    FROM {{ this }} as old_rec
    LEFT JOIN upd_exp_rec on old_rec.id = upd_exp_rec.id
    WHERE upd_exp_rec.id is null
)

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM remove_old_from_dim

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM upd_exp_rec

{% endif %}

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM {{ ref("inc_employees_stg_new") }}

