{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  p2p_trade_bk,
  qty,
  crypto_amt,
  crypto_currency,
  amount,
  fiat_amt,
  fiat_currency,
  price,
  price_usd,
  fee,
  fee_amount,
  fee_currency,
  type,
  executed_at at time zone 'Europe/Moscow' as executed_at,
  date(executed_at at time zone 'Europe/Moscow') as executed_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_p2p_trades') }}
where
  status = 'Completed'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
