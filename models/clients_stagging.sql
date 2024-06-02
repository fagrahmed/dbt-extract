
{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    on_schema_change='create',
    pre_hook='TRUNCATE TABLE {{ this }}'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

SELECT
    md5(random()::text || clock_timestamp()::text) as unique_id,  
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,      
    clientId,    
    md5(
        COALESCE(clientid, '') || '::' || COALESCE(planid, '') || '::' || COALESCE(status, '') || '::' ||
        COALESCE(clientname::text, '') || '::' || COALESCE(clienttype, '') || '::' || COALESCE(clientcode, '') || '::' ||
        COALESCE(suspended::text, '') || '::' || COALESCE(hasgroupwallet::text, '') || '::' || COALESCE(numofemployees::text, '') || '::' || 
        COALESCE(bankpaymentwalletid, '') || '::' || COALESCE(walletpaymentwalletid, '') || '::' || COALESCE(salaryadvanceaccesslevel, '')
    ) AS hash_column,

    clientname->>'en' as clientname_en,
    clienttype,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS client_createdat_utc2,
    (lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS client_modifiedat_utc2,

    status as client_status,
    industrytype,
    address->>'governorate' as address_governorate,
    address->>'city' as address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate

FROM {{source('axis_sme', 'clients') }} src

{% if is_incremental() and table_exists %}
    WHERE src._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'clients_dimension') }}), '1900-01-01'::timestamp)
{% endif %}