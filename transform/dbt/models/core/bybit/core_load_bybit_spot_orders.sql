{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='spot_order_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_spot_orders',
      order_id
    )
  ) as spot_order_bk,
  exec_id,
  block_trade_id,
  closed_size,
  exec_fee,
  exec_fee_v2,
  exec_price,
  exec_qty,
  exec_value,
  extra_fees,
  fee_rate,
  index_price,
  leaves_qty,
  mark_iv,
  mark_price,
  order_price,
  order_qty,
  trade_iv,
  underlying_price,
  exec_time_ms,
  exec_type,
  fee_currency,
  is_maker,
  market_unit,
  order_link_id,
  order_type,
  seq,
  side,
  stop_order_type,
  symbol,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_spot_orders') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
