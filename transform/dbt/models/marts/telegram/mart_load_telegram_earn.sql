{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_bk'
) }}

select
  earn_bk,
  asset,
  apy_pct,
  wallet_balance,
  price_usd,
  value_usd,
  yield_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_telegram_earn') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
