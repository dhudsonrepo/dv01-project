{{ config(materialized='view') }}

SELECT
    id AS loan_id,
    funded_amount,
    loan_amount,
    country_code,
    currency,
    activity,
    sector,
    date
FROM {{ source('raw', 'kiva_loans') }}