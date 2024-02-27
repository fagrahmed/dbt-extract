-- sme_dimensions/employees_dimension.sql

{{ config(materialized='table') }}


SELECT
    c.clientid,
    ce.mobilenumber as employee_mobile,
    ce.clientemployeeid as employee_id,
    ce.status as employee_status,
    (ce.createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as employee_createdat,
    (ce.lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as employee_lastmodifiedat,
    (ce.deletedtime::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as employee_deletedat,
    ce.salarytype as employee_salarytype,
    ce.tookfirstsalary,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ce.mobilenumber ORDER BY ce.lastmodifiedat DESC) = 1 THEN true
        ELSE false
    END AS currentflag,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ce.mobilenumber ORDER BY ce.lastmodifiedat DESC) = 1 THEN (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours')
        ELSE ce.lastmodifiedat::timestamptz
    END AS loaddate,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ce.mobilenumber ORDER BY ce.lastmodifiedat DESC) = 1 THEN null::timestamptz
        ELSE (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours')
    END AS expdate

FROM {{ source('axis_sme', 'clientemployees') }} ce
LEFT JOIN {{ source('axis_sme', 'clients') }} c ON ce.clientid = c.clientid
