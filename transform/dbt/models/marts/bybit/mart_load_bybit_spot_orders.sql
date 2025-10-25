{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='spot_order_bk'
) }}

select
  spot_order_bk,
  exec_fee,
  exec_price,
  exec_qty,
  exec_value,
  extra_fees,
  fee_rate,
  leaves_qty,
  order_price,
  order_qty,
  (to_timestamp(exec_time_ms / 1000.0) at time zone 'Europe/Moscow') as executed_at,
  exec_type,
  fee_currency,
  is_maker,
  order_type,
  side,
  symbol,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_spot_orders') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
