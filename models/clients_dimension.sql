-- sme_dimensions/clients_dimension.sql

{{ config(materialized='table') }}


SELECT
    md5(random()::text || clock_timestamp()::text) as id,
    clientId,
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
    bankpaymentwalletid,
    walletpaymentwalletid,

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{source('axis_sme', 'clients') }}
