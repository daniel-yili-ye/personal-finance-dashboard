-- This model now references the unified transactions from all sources
SELECT *
FROM {{ ref('stg__all_transactions') }}