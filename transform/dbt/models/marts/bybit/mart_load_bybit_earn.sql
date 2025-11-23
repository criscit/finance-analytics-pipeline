{{ config(
    materialized='view'
) }}

-- Current versions only from SCD2 snapshot
select
    earn_bk,
    asset,
    apy,
    wallet_balance,
    price_usd,
    value_usd,
    processed_at
from {{ ref('snap_bybit_earn') }}
where dbt_valid_to is null
