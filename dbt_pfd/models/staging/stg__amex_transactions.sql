{{ config(materialized='view') }}

SELECT 
    date,
    date_processed,
    description,
    cardmember,
    amount,
    foreign_spend_amount,
    commission,
    exchange_rate,
    merchant,
    additional_information,
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
    month
FROM {{ source('amex', 'transactions') }}
WHERE date IS NOT NULL  -- Filter out rows with failed date parsing 