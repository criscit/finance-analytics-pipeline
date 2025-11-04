{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_earn',
      asset
    )
  ) as earn_bk,
  asset,
  coalesce(apy_pct, 0) as apy_pct,
  coalesce(value_usd, 0) as value_usd,
  coalesce(yield_usd, 0) as yield_usd,
  coalesce(wallet_balance_coin, 0) as wallet_balance_coin,
  coalesce(price_per_coin_usdt, 0) as price_per_coin_usdt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_earn') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
