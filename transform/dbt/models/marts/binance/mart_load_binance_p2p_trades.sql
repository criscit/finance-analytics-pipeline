{{ config(
    materialized='incremental',
    unique_key='p2p_trade_bk'
) }}

select
  p2p_trade_bk,
  (to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  date(to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_dt,
  amount,
  commission,
  taker_amt,
  taker_commission,
  taker_commission_rate,
  total_price,
  unit_price,
  asset,
  asset_price_usd,
  fiat,
  pay_method_name,
  trade_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_p2p_trades') }}
where
  order_status = 'COMPLETED'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and p2p_trade_bk not in (
    select p2p_trade_bk from {{ this }}
  )
{% endif %}
