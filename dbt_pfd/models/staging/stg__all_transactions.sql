{{ config(materialized='view') }}

-- Union all transaction sources into a consistent schema
WITH amex_transactions AS (
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
        month,
        NULL as balance  -- AmEx doesn't have balance
    FROM {{ ref('stg__amex_transactions') }}
),

wealthsimple_transactions AS (
    SELECT 
        date,
        NULL as date_processed,  -- WealthSimple doesn't have date_processed
        description,
        NULL as cardmember,      -- WealthSimple doesn't have cardmember
        amount,
        NULL as foreign_spend_amount,  -- WealthSimple doesn't have foreign transactions
        NULL as commission,      -- WealthSimple doesn't have commission
        NULL as exchange_rate,   -- WealthSimple doesn't have exchange_rate
        NULL as merchant,        -- WealthSimple doesn't have merchant
        NULL as additional_information,  -- WealthSimple doesn't have additional_information
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
        balance
    FROM {{ ref('stg__wealthsimple_transactions') }}
)

SELECT * FROM amex_transactions
UNION ALL
SELECT * FROM wealthsimple_transactions 