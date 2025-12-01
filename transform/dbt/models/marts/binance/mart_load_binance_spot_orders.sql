{{ config(
    materialized='incremental',
    unique_key='spot_order_bk'
) }}

select
  spot_order_bk,
  pair,
  order_price,
  order_amt,
  executed_amt,
  average_price,
  trading_total,
  order_amt_currency,
  executed_amt_currency,
  average_price_currency,
  trading_total_currency,
  order_type,
  order_side,
  executed_price_usd,
  trading_currency,
  trading_price_usd,
  order_placed_at_utc at time zone 'Europe/Moscow' as order_placed_at,
  date(order_placed_at_utc at time zone 'Europe/Moscow') as order_placed_dt,
  executed_at_utc at time zone 'Europe/Moscow' as executed_at,
  date(executed_at_utc at time zone 'Europe/Moscow') as executed_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_spot_orders') }}
where
  order_status in ('FILLED', 'PARTIALLY_FILLED')
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and spot_order_bk not in (
    select spot_order_bk from {{ this }}
  )
{% endif %}
