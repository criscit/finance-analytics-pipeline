{{ config(
    materialized='view'
) }}

-- Current versions only from SCD2 snapshot
select
    asset_bk,
    coin,
    equity,
    free,
    locked,
    usd_value,
    wallet_balance,
    processed_at
from {{ ref('snap_bybit_assets') }}
where dbt_valid_to is null
