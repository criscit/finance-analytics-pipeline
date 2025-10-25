{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='asset_bk'
) }}

select
  asset_bk,
  asset,
  free,
  locked,
  total,
  usdt_equivalent,
  usdt_price,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_assets') }}
where
  total > 0
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
