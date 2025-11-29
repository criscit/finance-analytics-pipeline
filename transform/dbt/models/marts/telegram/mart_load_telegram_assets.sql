{{ config(materialized='table') }}

-- Current versions only from SCD2 snapshot
select
    asset_bk,
    asset_short_name,
    price_usd,
    balance,
    value_usd,
    processed_at
from {{ ref('snap_telegram_assets') }}
where dbt_valid_to is null
