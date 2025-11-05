{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  p2p_trade_bk,
  ad_type,
  role,
  net_crypto_amt,
  paid_fee_crypto_amt,
  fiat_amt,
  price,
  crypto_currency,
  fiat_currency,
  payment_method,
  created_at,
  date(created_at at time zone 'Europe/Moscow') as created_dt,
  completed_at,
  date(completed_at at time zone 'Europe/Moscow') as completed_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_telegram_p2p_trades') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
