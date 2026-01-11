{{ config(
    materialized='incremental',
    unique_key='fiat_payment_bk'
) }}

select
  fiat_payment_bk,
  executed_at_utc,
  obtain_amt,
  source_amt,
  price,
  total_fee,
  crypto_currency,
  crypto_price_usd,
  fiat_currency,
  payment_method,
  transaction_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_fiat_payments') }}
where
  status = 'Completed'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and fiat_payment_bk not in (
    select fiat_payment_bk from {{ this }}
  )
{% endif %}
