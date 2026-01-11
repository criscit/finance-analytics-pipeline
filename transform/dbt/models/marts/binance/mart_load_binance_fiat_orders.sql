{{ config(
    materialized='incremental',
    unique_key='fiat_order_bk'
) }}

select
  fiat_order_bk,
  executed_at_utc,
  amount,
  indicated_amt,
  total_fee,
  fiat_currency,
  price_usd,
  method,
  transaction_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_fiat_orders') }}
where
  status = 'Successful'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and fiat_order_bk not in (
    select fiat_order_bk from {{ this }}
  )
{% endif %}
