{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='fiat_order_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'binance_fiat_orders',
      order_no
    )
  ) as fiat_order_bk,
  to_timestamp(update_time_ms / 1000.0) as executed_at_utc,
  amount,
  indicated_amt,
  total_fee,
  fiat_currency,
  price_usd,
  method,
  status,
  transaction_type,

  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_binance_fiat_orders') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
