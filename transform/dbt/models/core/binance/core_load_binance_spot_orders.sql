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
  pair,
  order_price,
  order_amt,
  executed_amt,
  average_price,
  trading_total,
  order_type,
  order_side,
  order_status,
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
