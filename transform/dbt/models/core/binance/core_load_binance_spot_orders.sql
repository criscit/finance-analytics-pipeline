{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='spot_order_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'binance_spot_orders',
      order_no
    )
  ) as spot_order_bk,
  pair as pair,
  order_price,
  cast(regexp_extract(order_amt, '([0-9.]+)', 1) as double) as order_amt,
  cast(regexp_extract(executed_amt, '([0-9.]+)', 1) as double) as executed_amt,
  cast(regexp_extract(average_price, '([0-9.]+)', 1) as double) as average_price,
  cast(regexp_extract(trading_total, '([0-9.]+)', 1) as double) as trading_total,
  regexp_extract(order_amt, '[A-Z]+$', 0) as order_amt_currency,
  regexp_extract(executed_amt, '[A-Z]+$', 0) as executed_amt_currency,
  regexp_extract(average_price, '[A-Z]+$', 0) as average_price_currency,
  regexp_extract(trading_total, '[A-Z]+$', 0) as trading_total_currency,
  order_type,
  order_side,
  order_status,
  executed_price_usd,
  trading_currency,
  trading_price_usd,
  order_placed_at_utc,
  executed_at_utc,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_binance_spot_orders') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
