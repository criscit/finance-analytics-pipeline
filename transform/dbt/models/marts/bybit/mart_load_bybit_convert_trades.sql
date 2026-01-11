{{ config(
    materialized='incremental',
    unique_key='convert_trade_bk'
) }}

select
  convert_trade_bk,
  created_at_utc,
  exchange_rate,
  from_amt,
  from_coin,
  from_price_usd,
  to_amt,
  to_coin,
  to_price_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_convert_trades') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and convert_trade_bk not in (
    select convert_trade_bk from {{ this }}
  )
{% endif %}
