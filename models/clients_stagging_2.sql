{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    on_schema_change='create',
    pre_hook='TRUNCATE TABLE {{ this }}'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        stg.unique_id AS unique_id,
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
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.hash_column
            ELSE final.hash_column
        END AS hash_column,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.clientname_en
            ELSE final.clientname_en
        END AS clientname_en,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.clienttype
            ELSE final.clienttype
        END AS clienttype,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.client_createdat_utc2
            ELSE final.client_createdat_utc2
        END AS client_createdat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.client_modifiedat_utc2
            ELSE final.client_modifiedat_utc2
        END AS client_modifiedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.client_status
            ELSE final.client_status
        END AS client_status,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.industrytype
            ELSE final.industrytype
        END AS industrytype,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.address_governorate
            ELSE final.address_governorate
        END AS address_governorate,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.address_city
            ELSE final.address_city
        END AS address_city,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.numofemployees
            ELSE final.numofemployees
        END AS numofemployees,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.salaryadvanceaccesslevel
            ELSE final.salaryadvanceaccesslevel
        END AS salaryadvanceaccesslevel,
        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'clients_stagging') }} stg
    LEFT JOIN {{ source('dbt-dimensions', 'clients_dimension')}} final
        ON stg.clientid = final.clientid 
    WHERE final.hash_column is not null and final.operation != 'exp'
)

SELECT * from update_old

{% else %}

SELECT

    stg.unique_id AS unique_id,
    stg.operation AS operation,
    stg.currentflag AS currentflag,
    stg.expdate AS expdate,
    stg.clientid AS clientid,
    stg.hash_column AS hash_column,
    stg.clientname_en AS clientname_en,
    stg.clienttype AS clienttype,
    stg.client_createdat_utc2 AS client_createdat_utc2,
    stg.client_modifiedat_utc2 AS client_modifiedat_utc2,
    stg.client_status AS client_status,
    stg.industrytype AS industrytype,
    stg.address_governorate AS address_governorate,
    stg.address_city AS address_city,
    stg.numofemployees AS numofemployees,
    stg.salaryadvanceaccesslevel AS salaryadvanceaccesslevel,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate

FROM {{ source('dbt-dimensions', 'clients_stagging') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}

