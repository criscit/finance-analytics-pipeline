{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_p2p_trades',
      order_number
    )
  ) as p2p_trade_bk,
  ad_number,
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
  completed_at,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_p2p_trades') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
