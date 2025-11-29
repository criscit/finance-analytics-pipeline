{{ config(materialized='table') }}

-- Current versions only from SCD2 snapshot
select
    asset_bk,
    asset,
    free,
    locked,
    total,
    amount_usd,
    price_usd,
    processed_at
from {{ ref('snap_binance_assets') }}
where dbt_valid_to is null
  and total > 0
