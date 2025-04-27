{{ config(materialized='view') }}

SELECT
    id AS loan_id,
    loan_amnt AS loan_amount,
    funded_amnt,
    term,
    int_rate,
    grade,
    sub_grade,
    addr_state AS state,
    loan_status,
    issue_d AS issue_date,
    annual_inc,
    dti
FROM {{ source('raw', 'lending_club') }}