{{ config(
    materialized='incremental',
    unique_key='convert_trade_bk'
) }}

select
  convert_trade_bk,
  (to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  date(to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_dt,
  from_amt,
  from_asset,
  from_price_usd,
  to_amt,
  to_asset,
  to_price_usd,
  ratio,
  inverse_ratio,
  order_type,
  side,
  wallet_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_binance_convert_trades') }}
where
  order_status = 'SUCCESS'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and convert_trade_bk not in (
    select convert_trade_bk from {{ this }}
  )
{% endif %}
