{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='convert_trade_bk'
) }}

select
  convert_trade_bk,
  create_time_ms,
  (to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  from_amt,
  from_asset,
  to_amt,
  to_asset,
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
{% endif %}
