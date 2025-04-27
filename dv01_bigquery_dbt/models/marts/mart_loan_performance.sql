WITH kiva AS (
    SELECT
        loan_id,
        'kiva' AS source,
        funded_amount,
        loan_amount,
        country_code AS location,
    date AS issued_date
FROM {{ ref('stg_kiva_loans') }}
    ),

    lending_club AS (
SELECT
    loan_id,
    'lending_club' AS source,
    funded_amnt AS funded_amount,
    loan_amount,
    state AS location,
    PARSE_DATE('%b-%Y', issue_date) AS issued_date
FROM {{ ref('stg_lending_club_loans') }}
    )
SELECT *
FROM kiva
UNION ALL
SELECT *
FROM lending_club