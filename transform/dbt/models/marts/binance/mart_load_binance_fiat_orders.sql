{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='fiat_order_bk'
) }}

select
  fiat_order_bk,
  create_time_ms,
  (to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  (to_timestamp(update_time_ms / 1000.0) at time zone 'Europe/Moscow') as updated_at,
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
{% endif %}
