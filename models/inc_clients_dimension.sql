
{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='append_new_columns'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}


WITH upd_exp_rec AS (

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
)

{% if table_exists %}
, remove_old_from_dim AS (
    SELECT
        old_rec.id,
        old_rec.operation,
        old_rec.currentflag,
        old_rec.expdate,
        old_rec.clientId,
        old_rec.hash_column,
        old_rec.clientname_en,
        old_rec.clienttype,
        old_rec.client_createdat_local,
        old_rec.client_modifiedat_local,
        old_rec.utc,
        old_rec.client_status,
        old_rec.industrytype,
        old_rec.address_governorate,
        old_rec.address_city,
        old_rec.numofemployees,
        old_rec.salaryadvanceaccesslevel,
        old_rec.loaddate

    FROM {{ this }} AS old_rec
    LEFT JOIN upd_exp_rec ON old_rec.id = upd_exp_rec.id
    WHERE upd_exp_rec.id IS NULL
)

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
    client_status,
    industrytype,
    address_governorate,
    address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    loaddate

FROM remove_old_from_dim

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

FROM upd_exp_rec

{% endif %}

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

