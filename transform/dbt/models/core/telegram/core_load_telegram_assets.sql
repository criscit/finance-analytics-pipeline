{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='asset_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_assets',
      asset_full_name
    )
  ) as asset_bk,
  asset_full_name,
  asset_short_name,
  price_usd,
  change_24h_pct,
  balance,
  value_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_assets') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
