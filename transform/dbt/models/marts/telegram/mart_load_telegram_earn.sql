{{ config(
    materialized='view'
) }}

-- Current versions only from SCD2 snapshot
select
    earn_bk,
    asset,
    apy_pct,
    wallet_balance,
    price_usd,
    value_usd,
    yield_usd,
    processed_at
from {{ ref('snap_telegram_earn') }}
where dbt_valid_to is null
