
{{
    config(
        materialized="incremental",
        unique_key= ["hash_column"],
        on_schema_change='append_new_columns',
        incremental_strategy = 'merge'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('inc_clients_stg_update') %}
{% set _ = ref('inc_clients_stg_exp') %}
{% set _ = ref('inc_clients_stg_new') %}
{% set _ = ref('inc_clients_stg') %}


SELECT 
    id,  
    operation,
    currentflag,
    expdate,      
    clientId,    
    hash_column,
    clientname_en,
    clienttype,
    client_createdat_local,
    client_modifiedat_local,
    utc,
    client_status,
    industrytype,
    address_governorate,
    address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    loaddate

FROM {{ ref("inc_clients_stg_update") }} stg

UNION ALL

SELECT 
    id,  
    operation,
    currentflag,
    expdate,      
    clientId,    
    hash_column,
    clientname_en,
    clienttype,
    client_createdat_local,
    client_modifiedat_local,
    utc,
    client_status,
    industrytype,
    address_governorate,
    address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    loaddate

FROM {{ ref("inc_clients_stg_exp") }} 

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientId,
    hash_column,
    clientname_en,
    clienttype,
    client_createdat_local,
    client_modifiedat_local,
    utc,
    client_status,
    industrytype,
    address_governorate,
    address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    loaddate

FROM {{ref("inc_clients_stg_new")}}

