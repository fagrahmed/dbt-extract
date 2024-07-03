
{{ config(
    materialized='incremental',
    unique_key= ['employee_id', 'employee_mobile'],
    on_schema_change='create'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        stg.id AS id,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column AND final.operation = 'insert' THEN 'update'
            ELSE 'exp'
        END AS operation,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN true
            ELSE false
        END AS currentflag,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN null::timestamptz
            ELSE now()::timestamptz
        END AS expdate,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.clientid
            ELSE final.clientid
        END AS clientid,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_mobile
            ELSE final.employee_mobile
        END AS employee_mobile,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_id
            ELSE final.employee_id
        END AS employee_id,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.hash_column
            ELSE final.hash_column
        END AS hash_column,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_status
            ELSE final.employee_status
        END AS employee_status,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_createdat_utc2
            ELSE final.employee_createdat_utc2
        END AS employee_createdat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_lastmodifiedat_utc2
            ELSE final.employee_lastmodifiedat_utc2
        END AS employee_lastmodifiedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_deletedat_utc2
            ELSE final.employee_deletedat_utc2
        END AS employee_deletedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.employee_salarytype
            ELSE final.employee_salarytype
        END AS employee_salarytype,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.tookfirstsalary
            ELSE final.tookfirstsalary
        END AS tookfirstsalary,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.iseligibleforclaimrequest
            ELSE final.iseligibleforclaimrequest
        END AS iseligibleforclaimrequest,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.iseligibleforadvancerequest
            ELSE final.iseligibleforadvancerequest
        END AS iseligibleforadvancerequest,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.advancerequestrequiresapproval
            ELSE final.advancerequestrequiresapproval
        END AS advancerequestrequiresapproval,

        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'inc_employees_stagging') }} stg
    LEFT JOIN {{ source('dbt-dimensions', 'inc_employees_dimension')}} final
        ON stg.employee_id = final.employee_id AND stg.employee_mobile = final.employee_mobile
    WHERE final.hash_column is not null and final.operation != 'exp'
        AND stg.loaddate > final.loaddate
)

SELECT * from update_old

{% else %}

-- Do nothing in first load
SELECT *

FROM {{ source('dbt-dimensions', 'inc_employees_stagging') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}

