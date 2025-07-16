{{ config(materialized='view') }}

SELECT 
    date,
    NULL as date_processed,
    description,
    NULL as cardmember,
    amount,
    NULL as foreign_spend_amount,
    NULL as commission,
    NULL as exchange_rate,
    NULL as merchant,
    NULL as additional_information,
    year_month,
    flow_type,
    abs_amount,
    is_weekend,
    institution,
    transaction_hash,
    source_file,
    file_hash,
    loaded_at,
    year,
    month,
    balance  -- WealthSimple specific column
FROM {{ source('wealthsimple', 'transactions') }}
WHERE date IS NOT NULL  -- Filter out rows with failed date parsing 