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
  cast(nullif(closed_size, '') as double) as closed_size,
  exec_fee_v2 as exec_fee,
  exec_price,
  exec_qty,
  exec_value,
  extra_fees,
  fee_rate,
  cast(nullif(index_price, '') as double) as index_price,
  cast(nullif(leaves_qty, '') as double) as leaves_qty,
  cast(nullif(mark_iv, '') as double) as mark_iv,
  cast(nullif(mark_price, '') as double) as mark_price,
  order_price,
  order_qty,
  cast(nullif(trade_iv, '') as double) as trade_iv,
  cast(nullif(underlying_price, '') as double) as underlying_price,
  exec_time_ms,
  exec_type,
  fee_currency,
  cast(is_maker as boolean) as is_maker,
  market_unit,
  order_link_id,
  order_type,
  cast(nullif(seq, '') as bigint) as seq,
  side,
  stop_order_type,
  lower(symbol) as symbol,
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
