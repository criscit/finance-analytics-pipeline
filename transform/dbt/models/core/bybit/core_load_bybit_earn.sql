{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_earn',
      asset
    )
  ) as earn_bk,
  asset,
  coalesce(apy, 0) as apy_pct,
  coalesce(wallet_balance, 0) as wallet_balance,
  coalesce(price_usd, 0) as price_usd,
  coalesce(value_usd, 0) as value_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_earn') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
